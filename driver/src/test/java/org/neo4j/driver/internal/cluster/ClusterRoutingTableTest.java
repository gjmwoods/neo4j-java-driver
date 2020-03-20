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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;

import org.neo4j.connector.cluster.ClusterRoutingTable;
import org.neo4j.connector.cluster.RoutingTable;
import org.neo4j.connector.internal.BoltServerAddress;
import org.neo4j.connector.internal.util.Clock;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.ClusterCompositionUtil;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.connector.AccessMode.READ;
import static org.neo4j.connector.AccessMode.WRITE;
import static org.neo4j.connector.internal.DatabaseNameUtil.database;
import static org.neo4j.connector.internal.DatabaseNameUtil.defaultDatabase;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.createClusterComposition;

class ClusterRoutingTableTest
{
    @Test
    void shouldReturnStaleIfTtlExpired()
    {
        // Given
        FakeClock clock = new FakeClock();
        RoutingTable routingTable = newRoutingTable( clock );

        // When
        routingTable.update( ClusterCompositionUtil.createClusterComposition( 1000,
                                                                              asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ), asList( ClusterCompositionUtil.C ), asList( ClusterCompositionUtil.D, ClusterCompositionUtil.E ) ) );
        clock.progress( 1234 );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldReturnStaleIfNoRouter()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( ClusterCompositionUtil.createClusterComposition( ClusterCompositionUtil.EMPTY, asList( ClusterCompositionUtil.C ), asList( ClusterCompositionUtil.D, ClusterCompositionUtil.E ) ) );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeStaleForReadsButNotWritesWhenNoReaders()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( ClusterCompositionUtil
                                     .createClusterComposition( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ), asList( ClusterCompositionUtil.C ), ClusterCompositionUtil.EMPTY ) );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeStaleForWritesButNotReadsWhenNoWriters()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( ClusterCompositionUtil
                                     .createClusterComposition( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ), ClusterCompositionUtil.EMPTY, asList( ClusterCompositionUtil.D, ClusterCompositionUtil.E ) ) );

        // Then
        assertFalse( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeNotStaleWithReadersWritersAndRouters()
    {
        // Given
        RoutingTable routingTable = newRoutingTable();

        // When
        routingTable.update( ClusterCompositionUtil
                                     .createClusterComposition( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ), asList( ClusterCompositionUtil.C ), asList( ClusterCompositionUtil.D, ClusterCompositionUtil.E ) ) );

        // Then
        assertFalse( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldBeStaleForReadsAndWritesAfterCreation()
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), clock, ClusterCompositionUtil.A );

        // Then
        assertTrue( routingTable.isStaleFor( READ ) );
        assertTrue( routingTable.isStaleFor( WRITE ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"Molly", "", "I AM A NAME"} )
    void shouldReturnDatabaseNameCorrectly( String db )
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( database( db ), clock, ClusterCompositionUtil.A );

        // Then
        assertEquals( db, routingTable.database().description() );
    }

    @Test
    void shouldContainInitialRouters()
    {
        // Given
        FakeClock clock = new FakeClock();

        // When
        RoutingTable routingTable = new ClusterRoutingTable( defaultDatabase(), clock, ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C );

        // Then
        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.A, ClusterCompositionUtil.B, ClusterCompositionUtil.C}, routingTable.routers().toArray() );
        assertArrayEquals( new BoltServerAddress[0], routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[0], routingTable.writers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfRouters()
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        List<BoltServerAddress> routers = asList( ClusterCompositionUtil.A, ClusterCompositionUtil.C, ClusterCompositionUtil.D, ClusterCompositionUtil.F, ClusterCompositionUtil.B, ClusterCompositionUtil.E );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( routers, ClusterCompositionUtil.EMPTY, ClusterCompositionUtil.EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.A, ClusterCompositionUtil.C, ClusterCompositionUtil.D, ClusterCompositionUtil.F, ClusterCompositionUtil.B, ClusterCompositionUtil.E}, routingTable.routers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfWriters()
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        List<BoltServerAddress> writers = asList( ClusterCompositionUtil.D, ClusterCompositionUtil.F, ClusterCompositionUtil.A, ClusterCompositionUtil.C, ClusterCompositionUtil.E );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( ClusterCompositionUtil.EMPTY, writers, ClusterCompositionUtil.EMPTY ) );

        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.D, ClusterCompositionUtil.F, ClusterCompositionUtil.A, ClusterCompositionUtil.C, ClusterCompositionUtil.E}, routingTable.writers().toArray() );
    }

    @Test
    void shouldPreserveOrderingOfReaders()
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        List<BoltServerAddress> readers = asList( ClusterCompositionUtil.B, ClusterCompositionUtil.A, ClusterCompositionUtil.F, ClusterCompositionUtil.C, ClusterCompositionUtil.D );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( ClusterCompositionUtil.EMPTY, ClusterCompositionUtil.EMPTY, readers ) );

        assertArrayEquals( new BoltServerAddress[]{ClusterCompositionUtil.B, ClusterCompositionUtil.A, ClusterCompositionUtil.F, ClusterCompositionUtil.C, ClusterCompositionUtil.D}, routingTable.readers().toArray() );
    }

    @Test
    void shouldTreatOneRouterAsValid()
    {
        ClusterRoutingTable routingTable = newRoutingTable();

        List<BoltServerAddress> routers = singletonList( ClusterCompositionUtil.A );
        List<BoltServerAddress> writers = asList( ClusterCompositionUtil.B, ClusterCompositionUtil.C );
        List<BoltServerAddress> readers = asList( ClusterCompositionUtil.D, ClusterCompositionUtil.E );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( routers, writers, readers ) );

        assertFalse( routingTable.isStaleFor( READ ) );
        assertFalse( routingTable.isStaleFor( WRITE ) );
    }

    @Test
    void shouldHaveBeStaleForExpiredTime() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable( Clock.SYSTEM );
        assertTrue( routingTable.hasBeenStaleFor( 0 ) );
    }

    @Test
    void shouldNotHaveBeStaleForUnexpiredTime() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable( Clock.SYSTEM );
        assertFalse( routingTable.hasBeenStaleFor( Duration.ofSeconds( 30 ).toMillis() ) );
    }

    @Test
    void shouldDefaultToPreferInitialRouter() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        assertTrue( routingTable.preferInitialRouter() );
    }

    @Test
    void shouldPreferInitialRouterIfNoWriter() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        routingTable.update( ClusterCompositionUtil
                                     .createClusterComposition( ClusterCompositionUtil.EMPTY, ClusterCompositionUtil.EMPTY, ClusterCompositionUtil.EMPTY ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( singletonList( ClusterCompositionUtil.A ), ClusterCompositionUtil.EMPTY, singletonList( ClusterCompositionUtil.A ) ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil
                                     .createClusterComposition( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ), ClusterCompositionUtil.EMPTY, asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( ClusterCompositionUtil.EMPTY, ClusterCompositionUtil.EMPTY, singletonList( ClusterCompositionUtil.A ) ) );
        assertTrue( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( singletonList( ClusterCompositionUtil.A ), ClusterCompositionUtil.EMPTY, ClusterCompositionUtil.EMPTY ) );
        assertTrue( routingTable.preferInitialRouter() );
    }

    @Test
    void shouldNotPreferInitialRouterIfHasWriter() throws Throwable
    {
        ClusterRoutingTable routingTable = newRoutingTable();
        routingTable.update( ClusterCompositionUtil.createClusterComposition( ClusterCompositionUtil.EMPTY, singletonList( ClusterCompositionUtil.A ), ClusterCompositionUtil.EMPTY ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( singletonList( ClusterCompositionUtil.A ), singletonList( ClusterCompositionUtil.A ), singletonList( ClusterCompositionUtil.A ) ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ), singletonList( ClusterCompositionUtil.A ), asList( ClusterCompositionUtil.A, ClusterCompositionUtil.B ) ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( ClusterCompositionUtil.EMPTY, singletonList( ClusterCompositionUtil.A ), singletonList( ClusterCompositionUtil.A ) ) );
        assertFalse( routingTable.preferInitialRouter() );

        routingTable.update( ClusterCompositionUtil.createClusterComposition( singletonList( ClusterCompositionUtil.A ), singletonList( ClusterCompositionUtil.A ), ClusterCompositionUtil.EMPTY ) );
        assertFalse( routingTable.preferInitialRouter() );
    }

    private ClusterRoutingTable newRoutingTable()
    {
        return new ClusterRoutingTable( defaultDatabase(), new FakeClock() );
    }

    private ClusterRoutingTable newRoutingTable( Clock clock )
    {
        return new ClusterRoutingTable( defaultDatabase(), clock );
    }
}
