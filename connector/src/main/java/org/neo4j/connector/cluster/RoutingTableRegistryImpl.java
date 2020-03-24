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
package org.neo4j.connector.cluster;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.connector.Logger;
import org.neo4j.connector.internal.BoltServerAddress;
import org.neo4j.connector.DatabaseName;
import org.neo4j.connector.async.ConnectionContext;
import org.neo4j.connector.spi.ConnectionPool;
import org.neo4j.connector.internal.util.Clock;

public class RoutingTableRegistryImpl implements RoutingTableRegistry
{
    private final ConcurrentMap<DatabaseName,RoutingTableHandler> routingTableHandlers;
    private final RoutingTableHandlerFactory factory;
    private final Logger logger;

    public RoutingTableRegistryImpl( ConnectionPool connectionPool, Rediscovery rediscovery, Clock clock, Logger logger, long routingTablePurgeDelayMs )
    {
        this( new ConcurrentHashMap<>(), new RoutingTableHandlerFactory( connectionPool, rediscovery, clock, logger, routingTablePurgeDelayMs ), logger );
    }

    public RoutingTableRegistryImpl( ConcurrentMap<DatabaseName,RoutingTableHandler> routingTableHandlers, RoutingTableHandlerFactory factory, Logger logger )
    {
        this.factory = factory;
        this.routingTableHandlers = routingTableHandlers;
        this.logger = logger;
    }

    @Override
    public CompletionStage<RoutingTableHandler> ensureRoutingTable( ConnectionContext context )
    {
        RoutingTableHandler handler = getOrCreate( context.databaseName() );
        return handler.ensureRoutingTable( context ).thenApply( ignored -> handler );
    }

    @Override
    public Set<BoltServerAddress> allServers()
    {
        // obviously we just had a snapshot of all servers in all routing tables
        // after we read it, the set could already be changed.
        Set<BoltServerAddress> servers = new HashSet<>();
        for ( RoutingTableHandler tableHandler : routingTableHandlers.values() )
        {
            servers.addAll( tableHandler.servers() );
        }
        return servers;
    }

    @Override
    public void remove( DatabaseName databaseName )
    {
        routingTableHandlers.remove( databaseName );
        logger.debug( "Routing table handler for database '%s' is removed.", databaseName.description() );
    }

    @Override
    public void removeAged()
    {
        routingTableHandlers.forEach( ( databaseName, handler ) -> {
            if ( handler.isRoutingTableAged() )
            {
                logger.info( "Routing table handler for database '%s' is removed because it has not been used for a long time. Routing table: %s",
                        databaseName.description(), handler.routingTable() );
                routingTableHandlers.remove( databaseName );
            }
        } );
    }

    // For tests
    public boolean contains( DatabaseName databaseName )
    {
        return routingTableHandlers.containsKey( databaseName );
    }

    private RoutingTableHandler getOrCreate( DatabaseName databaseName )
    {
        return routingTableHandlers.computeIfAbsent( databaseName, name -> {
            RoutingTableHandler handler = factory.newInstance( name, this );
            logger.debug( "Routing table handler for database '%s' is added.", databaseName.description() );
            return handler;
        } );
    }

    public static class RoutingTableHandlerFactory //todo made public
    {
        private final ConnectionPool connectionPool;
        private final Rediscovery rediscovery;
        private final Logger log;
        private final Clock clock;
        private final long routingTablePurgeDelayMs;

        public RoutingTableHandlerFactory( ConnectionPool connectionPool, Rediscovery rediscovery, Clock clock, Logger log, long routingTablePurgeDelayMs ) //todo made public
        {
            this.connectionPool = connectionPool;
            this.rediscovery = rediscovery;
            this.clock = clock;
            this.log = log;
            this.routingTablePurgeDelayMs = routingTablePurgeDelayMs;
        }

        public RoutingTableHandler newInstance( DatabaseName databaseName, RoutingTableRegistry allTables ) //todo made public
        {
            ClusterRoutingTable routingTable = new ClusterRoutingTable( databaseName, clock );
            return new RoutingTableHandlerImpl( routingTable, rediscovery, connectionPool, allTables, log, routingTablePurgeDelayMs );
        }
    }
}
