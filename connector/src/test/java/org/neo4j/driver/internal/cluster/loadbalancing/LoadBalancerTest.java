/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.cluster.loadbalancing;

import io.netty.util.concurrent.GlobalEventExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.neo4j.driver.AccessMode;
import org.neo4j.connector.cluster.loadbalancing.LeastConnectedLoadBalancingStrategy;
import org.neo4j.connector.cluster.loadbalancing.LoadBalancer;
import org.neo4j.connector.exception.AuthenticationException;
import org.neo4j.connector.exception.SecurityException;
import org.neo4j.connector.exception.ServiceUnavailableException;
import org.neo4j.connector.exception.SessionExpiredException;
import org.neo4j.connector.internal.BoltServerAddress;
import org.neo4j.connector.async.ConnectionContext;
import org.neo4j.connector.async.connection.RoutingConnection;
import org.neo4j.connector.cluster.AddressSet;
import org.neo4j.connector.cluster.ClusterComposition;
import org.neo4j.connector.cluster.ClusterRoutingTable;
import org.neo4j.connector.cluster.Rediscovery;
import org.neo4j.connector.cluster.RoutingTable;
import org.neo4j.connector.cluster.RoutingTableHandler;
import org.neo4j.connector.cluster.RoutingTableRegistry;
import org.neo4j.connector.messaging.BoltProtocol;
import org.neo4j.connector.spi.Connection;
import org.neo4j.connector.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.connector.internal.util.Futures;
import org.neo4j.connector.internal.util.ServerVersion;
import org.neo4j.driver.internal.cluster.RediscoveryUtil;
import org.neo4j.driver.internal.util.ClusterCompositionUtil;
import org.neo4j.driver.util.TestUtil;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.connector.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.connector.async.ImmutableConnectionContext.simple;
import static org.neo4j.connector.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.connector.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.connector.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.util.TestUtil.await;

class LoadBalancerTest
{
    @ParameterizedTest
    @EnumSource( AccessMode.class )
    void returnsCorrectAccessMode( AccessMode mode )
    {
        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet readerAddresses = mock( AddressSet.class );
        AddressSet writerAddresses = mock( AddressSet.class );
        when( readerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{ClusterCompositionUtil.A} );
        when( writerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{ClusterCompositionUtil.B} );
        when( routingTable.readers() ).thenReturn( readerAddresses );
        when( routingTable.writers() ).thenReturn( writerAddresses );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Connection acquired = TestUtil.await( loadBalancer.acquireConnection( RediscoveryUtil.contextWithMode( mode ) ) );

        assertThat( acquired, instanceOf( RoutingConnection.class ) );
        assertThat( acquired.mode(), equalTo( mode ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"", "foo", "data"} )
    void returnsCorrectDatabaseName( String databaseName )
    {
        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet writerAddresses = mock( AddressSet.class );
        when( writerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{ClusterCompositionUtil.A} );
        when( routingTable.writers() ).thenReturn( writerAddresses );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Connection acquired = TestUtil.await( loadBalancer.acquireConnection( RediscoveryUtil.contextWithDatabase( databaseName ) ) );

        assertThat( acquired, instanceOf( RoutingConnection.class ) );
        assertThat( acquired.databaseName().description(), equalTo( databaseName ) );
        verify( connectionPool ).acquire( ClusterCompositionUtil.A );
    }

    @Test
    void shouldThrowWhenRediscoveryReturnsNoSuitableServers()
    {
        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.readers() ).thenReturn( new AddressSet() );
        when( routingTable.writers() ).thenReturn( new AddressSet() );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        SessionExpiredException error1 = assertThrows( SessionExpiredException.class, () -> TestUtil
                .await( loadBalancer.acquireConnection( RediscoveryUtil.contextWithMode( READ ) ) ) );
        assertThat( error1.getMessage(), startsWith( "Failed to obtain connection towards READ server" ) );

        SessionExpiredException error2 = assertThrows( SessionExpiredException.class, () -> TestUtil
                .await( loadBalancer.acquireConnection( RediscoveryUtil.contextWithMode( WRITE ) ) ) );
        assertThat( error2.getMessage(), startsWith( "Failed to obtain connection towards WRITE server" ) );
    }

    @Test
    void shouldSelectLeastConnectedAddress()
    {
        ConnectionPool connectionPool = newConnectionPoolMock();

        when( connectionPool.inUseConnections( ClusterCompositionUtil.A ) ).thenReturn( 0 );
        when( connectionPool.inUseConnections( ClusterCompositionUtil.B ) ).thenReturn( 20 );
        when( connectionPool.inUseConnections( ClusterCompositionUtil.C ) ).thenReturn( 0 );

        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet readerAddresses = mock( AddressSet.class );
        when( readerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C} );
        when( routingTable.readers() ).thenReturn( readerAddresses );


        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Set<BoltServerAddress> seenAddresses = new HashSet<>();
        for ( int i = 0; i < 10; i++ )
        {
            Connection connection = TestUtil.await( loadBalancer.acquireConnection( newBoltV4ConnectionContext() ) );
            seenAddresses.add( connection.serverAddress() );
        }

        // server B should never be selected because it has many active connections
        assertEquals( 2, seenAddresses.size() );
        assertTrue( seenAddresses.containsAll( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.C ) ) );
    }

    @Test
    void shouldRoundRobinWhenNoActiveConnections()
    {
        ConnectionPool connectionPool = newConnectionPoolMock();

        RoutingTable routingTable = mock( RoutingTable.class );
        AddressSet readerAddresses = mock( AddressSet.class );
        when( readerAddresses.toArray() ).thenReturn( new BoltServerAddress[]{ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C} );
        when( routingTable.readers() ).thenReturn( readerAddresses );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Set<BoltServerAddress> seenAddresses = new HashSet<>();
        for ( int i = 0; i < 10; i++ )
        {
            Connection connection = TestUtil.await( loadBalancer.acquireConnection( newBoltV4ConnectionContext() ) );
            seenAddresses.add( connection.serverAddress() );
        }

        assertEquals( 3, seenAddresses.size() );
        assertTrue( seenAddresses.containsAll( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C ) ) );
    }

    @Test
    void shouldTryMultipleServersAfterRediscovery()
    {
        Set<BoltServerAddress> unavailableAddresses = TestUtil.asOrderedSet( ClusterCompositionUtil.A );
        ConnectionPool connectionPool = newConnectionPoolMockWithFailures( unavailableAddresses );

        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), new FakeClock() );
        routingTable.update( new ClusterComposition( -1, new LinkedHashSet<>( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) ), emptySet(), emptySet() ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTable );

        Connection connection = TestUtil.await( loadBalancer.acquireConnection( newBoltV4ConnectionContext() ) );

        assertNotNull( connection );
        Assertions.assertEquals( ClusterCompositionUtil.B, connection.serverAddress() );
        // routing table should've forgotten A
        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.B}, routingTable.readers().toArray() );
    }

    @Test
    void shouldFailWithResolverError() throws Throwable
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenThrow( new RuntimeException( "hi there" ) );

        LoadBalancer loadBalancer = newLoadBalancer( pool, rediscovery );

        RuntimeException exception = assertThrows( RuntimeException.class, () -> TestUtil.await( loadBalancer.supportsMultiDb() ) );
        assertThat( exception.getMessage(), equalTo( "hi there" ) );
    }

    @Test
    void shouldFailAfterTryingAllServers() throws Throwable
    {
        Set<BoltServerAddress> unavailableAddresses = TestUtil.asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.B );
        ConnectionPool connectionPool = newConnectionPoolMockWithFailures( unavailableAddresses );

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, rediscovery );

        ServiceUnavailableException exception = assertThrows( ServiceUnavailableException.class, () -> TestUtil.await( loadBalancer.supportsMultiDb() ) );
        Throwable[] suppressed = exception.getSuppressed();
        assertThat( suppressed.length, equalTo( 2 ) ); // one for A, one for B
        assertThat( suppressed[0].getMessage(), containsString( ClusterCompositionUtil.A.toString() ) );
        assertThat( suppressed[1].getMessage(), containsString( ClusterCompositionUtil.B.toString() ) );
        verify( connectionPool, times( 2 ) ).acquire( any() );
    }

    @Test
    void shouldFailEarlyOnSecurityError() throws Throwable
    {
        Set<BoltServerAddress> unavailableAddresses = TestUtil.asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.B );
        ConnectionPool connectionPool = newConnectionPoolMockWithFailures( unavailableAddresses, address -> new SecurityException( "code", "hi there" ) );

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, rediscovery );

        SecurityException exception = assertThrows( SecurityException.class, () -> TestUtil.await( loadBalancer.supportsMultiDb() ) );
        assertThat( exception.getMessage(), startsWith( "hi there" ) );
        verify( connectionPool, times( 1 ) ).acquire( any() );
    }

    @Test
    void shouldSuccessOnFirstSuccessfulServer() throws Throwable
    {
        Set<BoltServerAddress> unavailableAddresses = TestUtil.asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.B );
        ConnectionPool connectionPool = newConnectionPoolMockWithFailures( unavailableAddresses );

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C, ClusterCompositionUtil.D ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, rediscovery );

        Assertions.assertTrue( TestUtil.await( loadBalancer.supportsMultiDb() ) );
        verify( connectionPool, times( 3 ) ).acquire( any() );
    }

    @Test
    void shouldThrowModifiedErrorWhenSupportMultiDbTestFails() throws Throwable
    {
        Set<BoltServerAddress> unavailableAddresses = TestUtil.asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.B );
        ConnectionPool connectionPool = newConnectionPoolMockWithFailures( unavailableAddresses );

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, rediscovery );

        ServiceUnavailableException exception = assertThrows( ServiceUnavailableException.class, () -> TestUtil.await( loadBalancer.verifyConnectivity() ) );
        assertThat( exception.getMessage(), startsWith( "Unable to connect to database management service," ) );
    }

    @Test
    void shouldFailEarlyOnSecurityErrorWhenSupportMultiDbTestFails() throws Throwable
    {
        Set<BoltServerAddress> unavailableAddresses = TestUtil.asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.B );
        ConnectionPool connectionPool = newConnectionPoolMockWithFailures( unavailableAddresses, address -> new AuthenticationException( "code", "error" ) );

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, rediscovery );

        AuthenticationException exception = assertThrows( AuthenticationException.class, () -> TestUtil.await( loadBalancer.verifyConnectivity() ) );
        assertThat( exception.getMessage(), startsWith( "error" ) );
    }

    @Test
    void shouldThrowModifiedErrorWhenRefreshRoutingTableFails() throws Throwable
    {
        ConnectionPool connectionPool = newConnectionPoolMock();

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) );

        RoutingTableRegistry routingTables = mock( RoutingTableRegistry.class );
        when( routingTables.ensureRoutingTable( any( ConnectionContext.class ) ) ).thenThrow( new ServiceUnavailableException( "boooo" ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables, rediscovery );

        ServiceUnavailableException exception = assertThrows( ServiceUnavailableException.class, () -> TestUtil.await( loadBalancer.verifyConnectivity() ) );
        assertThat( exception.getMessage(), startsWith( "Unable to connect to database management service," ) );
        verify( routingTables ).ensureRoutingTable( any( ConnectionContext.class ) );
    }

    @Test
    void shouldThrowOriginalErrorWhenRefreshRoutingTableFails() throws Throwable
    {
        ConnectionPool connectionPool = newConnectionPoolMock();

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) );

        RoutingTableRegistry routingTables = mock( RoutingTableRegistry.class );
        when( routingTables.ensureRoutingTable( any( ConnectionContext.class ) ) ).thenThrow( new RuntimeException( "boo" ) );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables, rediscovery );

        RuntimeException exception = assertThrows( RuntimeException.class, () -> TestUtil.await( loadBalancer.verifyConnectivity() ) );
        assertThat( exception.getMessage(), startsWith( "boo" ) );
        verify( routingTables ).ensureRoutingTable( any( ConnectionContext.class ) );
    }

    @Test
    void shouldReturnSuccessVerifyConnectivity() throws Throwable
    {
        ConnectionPool connectionPool = newConnectionPoolMock();

        Rediscovery rediscovery = mock( Rediscovery.class );
        when( rediscovery.resolve() ).thenReturn( Arrays.asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) );

        RoutingTableRegistry routingTables = mock( RoutingTableRegistry.class );
        when( routingTables.ensureRoutingTable( any( ConnectionContext.class ) ) ).thenReturn( Futures.completedWithNull() );

        LoadBalancer loadBalancer = newLoadBalancer( connectionPool, routingTables, rediscovery );

        TestUtil.await( loadBalancer.verifyConnectivity() );
        verify( routingTables ).ensureRoutingTable( any( ConnectionContext.class ) );
    }

    private static ConnectionPool newConnectionPoolMock()
    {
        return newConnectionPoolMockWithFailures( emptySet() );
    }

    private static ConnectionPool newConnectionPoolMockWithFailures( Set<BoltServerAddress> unavailableAddresses )
    {
        return newConnectionPoolMockWithFailures( unavailableAddresses, address -> new ServiceUnavailableException( address + " is unavailable!" ) );
    }

    private static ConnectionPool newConnectionPoolMockWithFailures( Set<BoltServerAddress> unavailableAddresses, Function<BoltServerAddress, Throwable> errorAction )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).then( invocation ->
        {
            BoltServerAddress requestedAddress = invocation.getArgument( 0 );
            if ( unavailableAddresses.contains( requestedAddress ) )
            {
                return Futures.failedFuture( errorAction.apply( requestedAddress ) );
            }

            return completedFuture( newBoltV4Connection( requestedAddress ) );
        } );
        return pool;
    }

    private static Connection newBoltV4Connection( BoltServerAddress address )
    {
        Connection connection = mock( Connection.class );
        when( connection.serverAddress() ).thenReturn( address );
        when( connection.protocol() ).thenReturn( BoltProtocol.forVersion( 4 ) );
        when( connection.serverVersion() ).thenReturn( ServerVersion.v4_0_0 );
        when( connection.release() ).thenReturn( completedWithNull() );
        return connection;
    }

    private static ConnectionContext newBoltV4ConnectionContext()
    {
        return simple( true );
    }

    private static LoadBalancer newLoadBalancer( ConnectionPool connectionPool, RoutingTable routingTable )
    {
        // Used only in testing
        RoutingTableRegistry routingTables = mock( RoutingTableRegistry.class );
        RoutingTableHandler handler = mock( RoutingTableHandler.class );
        when( handler.routingTable() ).thenReturn( routingTable );
        when( routingTables.ensureRoutingTable( any( ConnectionContext.class ) ) ).thenReturn( CompletableFuture.completedFuture( handler ) );
        Rediscovery rediscovery = mock( Rediscovery.class );
        return new LoadBalancer( connectionPool, routingTables, rediscovery, new LeastConnectedLoadBalancingStrategy( connectionPool, DEV_NULL_LOGGING ),
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGER );
    }

    private static LoadBalancer newLoadBalancer( ConnectionPool connectionPool, Rediscovery rediscovery )
    {
        // Used only in testing
        RoutingTableRegistry routingTables = mock( RoutingTableRegistry.class );
        return newLoadBalancer( connectionPool, routingTables, rediscovery );
    }

    private static LoadBalancer newLoadBalancer( ConnectionPool connectionPool, RoutingTableRegistry routingTables, Rediscovery rediscovery )
    {
        // Used only in testing
        return new LoadBalancer( connectionPool, routingTables, rediscovery, new LeastConnectedLoadBalancingStrategy( connectionPool, DEV_NULL_LOGGING ),
                GlobalEventExecutor.INSTANCE, DEV_NULL_LOGGER );
    }
}
