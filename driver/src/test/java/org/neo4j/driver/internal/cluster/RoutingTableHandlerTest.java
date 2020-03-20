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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.neo4j.connector.AccessMode;
import org.neo4j.connector.cluster.AddressSet;
import org.neo4j.connector.cluster.ClusterComposition;
import org.neo4j.connector.cluster.ClusterRoutingTable;
import org.neo4j.connector.cluster.Rediscovery;
import org.neo4j.connector.cluster.RediscoveryImpl;
import org.neo4j.connector.cluster.RoutingTable;
import org.neo4j.connector.cluster.RoutingTableHandler;
import org.neo4j.connector.cluster.RoutingTableHandlerImpl;
import org.neo4j.connector.cluster.RoutingTableRegistry;
import org.neo4j.connector.exception.ServiceUnavailableException;
import org.neo4j.connector.internal.BoltServerAddress;
import org.neo4j.connector.DatabaseName;
import org.neo4j.connector.internal.InternalBookmark;
import org.neo4j.connector.async.ConnectionContext;
import org.neo4j.connector.spi.Connection;
import org.neo4j.connector.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.connector.internal.util.Futures;
import org.neo4j.driver.internal.util.ClusterCompositionUtil;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.connector.AccessMode.READ;
import static org.neo4j.connector.AccessMode.WRITE;
import static org.neo4j.connector.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.connector.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.connector.async.ImmutableConnectionContext.simple;
import static org.neo4j.connector.cluster.RoutingSettings.STALE_ROUTING_TABLE_PURGE_DELAY_MS;
import static org.neo4j.connector.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.util.TestUtil.asOrderedSet;
import static org.neo4j.driver.util.TestUtil.await;

class RoutingTableHandlerTest
{
    @Test
    void shouldRemoveAddressFromRoutingTableOnConnectionFailure()
    {
        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), new FakeClock() );
        routingTable.update( new ClusterComposition(
                42, asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C ), asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.C, ClusterCompositionUtil.E ), asOrderedSet( ClusterCompositionUtil.B, ClusterCompositionUtil.D, ClusterCompositionUtil.F ) ) );

        RoutingTableHandler handler = newRoutingTableHandler( routingTable, newRediscoveryMock(), newConnectionPoolMock() );


        handler.onConnectionFailure( ClusterCompositionUtil.B );

        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.A, ClusterCompositionUtil.C}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.A, ClusterCompositionUtil.C, ClusterCompositionUtil.E}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.D, ClusterCompositionUtil.F}, routingTable.routers().toArray() );

        handler.onConnectionFailure( ClusterCompositionUtil.A );

        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.C}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.C, ClusterCompositionUtil.E}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.D, ClusterCompositionUtil.F}, routingTable.routers().toArray() );
    }

    @Test
    void acquireShouldUpdateRoutingTableWhenKnownRoutingTableIsStale()
    {
        BoltServerAddress initialRouter = new BoltServerAddress( "initialRouter", 1 );
        BoltServerAddress reader1 = new BoltServerAddress( "reader-1", 2 );
        BoltServerAddress reader2 = new BoltServerAddress( "reader-1", 3 );
        BoltServerAddress writer1 = new BoltServerAddress( "writer-1", 4 );
        BoltServerAddress router1 = new BoltServerAddress( "router-1", 5 );

        ConnectionPool connectionPool = newConnectionPoolMock();
        ClusterRoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), new FakeClock(), initialRouter );

        Set<BoltServerAddress> readers = new LinkedHashSet<>( asList( reader1, reader2 ) );
        Set<BoltServerAddress> writers = new LinkedHashSet<>( singletonList( writer1 ) );
        Set<BoltServerAddress> routers = new LinkedHashSet<>( singletonList( router1 ) );
        ClusterComposition clusterComposition = new ClusterComposition( 42, readers, writers, routers );
        Rediscovery rediscovery = mock( RediscoveryImpl.class );
        when( rediscovery.lookupClusterComposition( eq( routingTable ), eq( connectionPool ), any() ) )
                .thenReturn( completedFuture( clusterComposition ) );

        RoutingTableHandler handler = newRoutingTableHandler( routingTable, rediscovery, connectionPool );

        assertNotNull( await( handler.ensureRoutingTable( simple( false ) ) ) );

        verify( rediscovery ).lookupClusterComposition( eq ( routingTable ) , eq ( connectionPool ), any() );
        assertArrayEquals( new BoltServerAddress[]{reader1, reader2}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{writer1}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{router1}, routingTable.routers().toArray() );
    }

    @Test
    void shouldRediscoverOnReadWhenRoutingTableIsStaleForReads()
    {
        testRediscoveryWhenStale( READ );
    }

    @Test
    void shouldRediscoverOnWriteWhenRoutingTableIsStaleForWrites()
    {
        testRediscoveryWhenStale( WRITE );
    }

    @Test
    void shouldNotRediscoverOnReadWhenRoutingTableIsStaleForWritesButNotReads()
    {
        testNoRediscoveryWhenNotStale( WRITE, READ );
    }

    @Test
    void shouldNotRediscoverOnWriteWhenRoutingTableIsStaleForReadsButNotWrites()
    {
        testNoRediscoveryWhenNotStale( READ, WRITE );
    }

    @Test
    void shouldRetainAllFetchedAddressesInConnectionPoolAfterFetchingOfRoutingTable()
    {
        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), new FakeClock() );
        routingTable.update( new ClusterComposition(
                42, asOrderedSet(), asOrderedSet( ClusterCompositionUtil.B, ClusterCompositionUtil.C ), asOrderedSet( ClusterCompositionUtil.D, ClusterCompositionUtil.E ) ) );

        ConnectionPool connectionPool = newConnectionPoolMock();

        Rediscovery rediscovery = newRediscoveryMock();
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( completedFuture(
                new ClusterComposition( 42, asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.B ), asOrderedSet( ClusterCompositionUtil.B, ClusterCompositionUtil.C ), asOrderedSet( ClusterCompositionUtil.A, ClusterCompositionUtil.C ) ) ) );

        RoutingTableRegistry registry = new RoutingTableRegistry()
        {
            @Override
            public CompletionStage<RoutingTableHandler> ensureRoutingTable( ConnectionContext context )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<BoltServerAddress> allServers()
            {
                return routingTable.servers();
            }

            @Override
            public void remove( DatabaseName databaseName )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void removeAged()
            {
            }
        };

        RoutingTableHandler handler = newRoutingTableHandler( routingTable, rediscovery, connectionPool, registry );

        RoutingTable actual = await( handler.ensureRoutingTable( simple( false ) ) );
        assertEquals( routingTable, actual );

        verify( connectionPool ).retainAll( new HashSet<>( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C ) ) );
    }

    @Test
    void shouldRemoveRoutingTableHandlerIfFailedToLookup() throws Throwable
    {
        // Given
        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), new FakeClock() );

        Rediscovery rediscovery = newRediscoveryMock();
        when( rediscovery.lookupClusterComposition( any(), any(), any() ) ).thenReturn( Futures.failedFuture( new RuntimeException( "Bang!" ) ) );

        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTableRegistry registry = newRoutingTableRegistryMock();
        // When

        RoutingTableHandler handler = newRoutingTableHandler( routingTable, rediscovery, connectionPool, registry );
        assertThrows( RuntimeException.class, () -> await( handler.ensureRoutingTable( simple( false ) ) ) );

        // Then
        verify( registry ).remove( defaultDatabase() );
    }

    private void testRediscoveryWhenStale( AccessMode mode )
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        when( connectionPool.acquire( LOCAL_DEFAULT ) )
                .thenReturn( completedFuture( mock( Connection.class ) ) );

        RoutingTable routingTable = newStaleRoutingTableMock( mode );
        Rediscovery rediscovery = newRediscoveryMock();

        RoutingTableHandler handler = newRoutingTableHandler( routingTable, rediscovery, connectionPool );
        RoutingTable actual = await( handler.ensureRoutingTable( RediscoveryUtil.contextWithMode( mode ) ) );
        assertEquals( routingTable, actual );

        verify( routingTable ).isStaleFor( mode );
        verify( rediscovery ).lookupClusterComposition( eq( routingTable ), eq( connectionPool ), any() );
    }

    private void testNoRediscoveryWhenNotStale( AccessMode staleMode, AccessMode notStaleMode )
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        when( connectionPool.acquire( LOCAL_DEFAULT ) )
                .thenReturn( completedFuture( mock( Connection.class ) ) );

        RoutingTable routingTable = newStaleRoutingTableMock( staleMode );
        Rediscovery rediscovery = newRediscoveryMock();

        RoutingTableHandler handler = newRoutingTableHandler( routingTable, rediscovery, connectionPool );

        assertNotNull( await( handler.ensureRoutingTable( RediscoveryUtil.contextWithMode( notStaleMode ) ) ) );
        verify( routingTable ).isStaleFor( notStaleMode );
        verify( rediscovery, never() ).lookupClusterComposition( eq( routingTable ), eq( connectionPool ), any() );
    }

    private static RoutingTable newStaleRoutingTableMock( AccessMode mode )
    {
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.isStaleFor( mode ) ).thenReturn( true );

        AddressSet addresses = new AddressSet();
        addresses.update( new HashSet<>( singletonList( LOCAL_DEFAULT ) ) );
        when( routingTable.readers() ).thenReturn( addresses );
        when( routingTable.writers() ).thenReturn( addresses );
        when( routingTable.database() ).thenReturn( defaultDatabase() );

        return routingTable;
    }

    private static RoutingTableRegistry newRoutingTableRegistryMock()
    {
        return mock( RoutingTableRegistry.class );
    }

    private static Rediscovery newRediscoveryMock()
    {
        Rediscovery rediscovery = mock( RediscoveryImpl.class );
        Set<BoltServerAddress> noServers = Collections.emptySet();
        ClusterComposition clusterComposition = new ClusterComposition( 1, noServers, noServers, noServers );
        when( rediscovery.lookupClusterComposition( any( RoutingTable.class ), any( ConnectionPool.class ), any( InternalBookmark.class ) ) )
                .thenReturn( completedFuture( clusterComposition ) );
        return rediscovery;
    }

    private static ConnectionPool newConnectionPoolMock()
    {
        return newConnectionPoolMockWithFailures( emptySet() );
    }

    private static ConnectionPool newConnectionPoolMockWithFailures(
            Set<BoltServerAddress> unavailableAddresses )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).then( invocation ->
        {
            BoltServerAddress requestedAddress = invocation.getArgument( 0 );
            if ( unavailableAddresses.contains( requestedAddress ) )
            {
                return Futures.failedFuture( new ServiceUnavailableException( requestedAddress + " is unavailable!" ) );
            }
            Connection connection = mock( Connection.class );
            when( connection.serverAddress() ).thenReturn( requestedAddress );
            return completedFuture( connection );
        } );
        return pool;
    }

    private static RoutingTableHandler newRoutingTableHandler( RoutingTable routingTable, Rediscovery rediscovery, ConnectionPool connectionPool )
    {
        return new RoutingTableHandlerImpl( routingTable, rediscovery, connectionPool, newRoutingTableRegistryMock(), DEV_NULL_LOGGER,
                                            STALE_ROUTING_TABLE_PURGE_DELAY_MS );
    }

    private static RoutingTableHandler newRoutingTableHandler( RoutingTable routingTable, Rediscovery rediscovery, ConnectionPool connectionPool,
            RoutingTableRegistry routingTableRegistry )
    {
        return new RoutingTableHandlerImpl( routingTable, rediscovery, connectionPool, routingTableRegistry, DEV_NULL_LOGGER, STALE_ROUTING_TABLE_PURGE_DELAY_MS );
    }
}
