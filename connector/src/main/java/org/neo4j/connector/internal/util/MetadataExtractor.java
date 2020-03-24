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
package org.neo4j.connector.internal.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.connector.Bookmark;
import org.neo4j.connector.Query;
import org.neo4j.connector.Value;
import org.neo4j.connector.exception.UntrustedServerException;
import org.neo4j.connector.internal.InternalBookmark;
import org.neo4j.connector.internal.summary.InternalDatabaseInfo;
import org.neo4j.connector.internal.summary.InternalNotification;
import org.neo4j.connector.internal.summary.InternalPlan;
import org.neo4j.connector.internal.summary.InternalProfiledPlan;
import org.neo4j.connector.internal.summary.InternalResultSummary;
import org.neo4j.connector.internal.summary.InternalServerInfo;
import org.neo4j.connector.internal.summary.InternalSummaryCounters;
import org.neo4j.connector.internal.types.InternalTypeSystem;
import org.neo4j.connector.spi.Connection;
import org.neo4j.connector.summary.ResultSummary;
import org.neo4j.connector.summary.summary.DatabaseInfo;
import org.neo4j.connector.summary.summary.Notification;
import org.neo4j.connector.summary.summary.Plan;
import org.neo4j.connector.summary.summary.ProfiledPlan;
import org.neo4j.connector.summary.summary.QueryType;
import org.neo4j.connector.summary.summary.ServerInfo;

import static java.util.Collections.emptyList;
import static org.neo4j.connector.internal.summary.InternalDatabaseInfo.DEFAULT_DATABASE_INFO;

public class MetadataExtractor
{
    public static final int ABSENT_QUERY_ID = -1;
    private final String resultAvailableAfterMetadataKey;
    private final String resultConsumedAfterMetadataKey;

    public MetadataExtractor( String resultAvailableAfterMetadataKey, String resultConsumedAfterMetadataKey )
    {
        this.resultAvailableAfterMetadataKey = resultAvailableAfterMetadataKey;
        this.resultConsumedAfterMetadataKey = resultConsumedAfterMetadataKey;
    }

    public List<String> extractQueryKeys(Map<String,Value> metadata )
    {
        Value keysValue = metadata.get( "fields" );
        if ( keysValue != null )
        {
            if ( !keysValue.isEmpty() )
            {
                List<String> keys = new ArrayList<>( keysValue.size() );
                for ( Value value : keysValue.values() )
                {
                    keys.add( value.asString() );
                }

                return keys;
            }
        }
        return emptyList();
    }

    public long extractQueryId( Map<String,Value> metadata )
    {
        Value queryId = metadata.get( "qid" );
        if ( queryId != null )
        {
            return queryId.asLong();
        }
        return ABSENT_QUERY_ID;
    }


    public long extractResultAvailableAfter( Map<String,Value> metadata )
    {
        Value resultAvailableAfterValue = metadata.get( resultAvailableAfterMetadataKey );
        if ( resultAvailableAfterValue != null )
        {
            return resultAvailableAfterValue.asLong();
        }
        return -1;
    }

    public ResultSummary extractSummary( Query query, Connection connection, long resultAvailableAfter, Map<String,Value> metadata )
    {
        ServerInfo serverInfo = new InternalServerInfo( connection.serverAddress(), connection.serverVersion() );
        DatabaseInfo dbInfo = extractDatabaseInfo( metadata );
        return new InternalResultSummary( query, serverInfo, dbInfo, extractQueryType( metadata ), extractCounters( metadata ), extractPlan( metadata ),
                                          extractProfiledPlan( metadata ), extractNotifications( metadata ), resultAvailableAfter,
                                          extractResultConsumedAfter( metadata, resultConsumedAfterMetadataKey ) );
    }

    public static DatabaseInfo extractDatabaseInfo( Map<String,Value> metadata )
    {
        Value dbValue = metadata.get( "db" );
        if ( dbValue == null || dbValue.isNull() )
        {
            return InternalDatabaseInfo.DEFAULT_DATABASE_INFO;
        }
        else
        {
            return new InternalDatabaseInfo( dbValue.asString() );
        }
    }

    public static Bookmark extractBookmarks( Map<String,Value> metadata )
    {
        Value bookmarkValue = metadata.get( "bookmark" );
        if ( bookmarkValue != null && !bookmarkValue.isNull() && bookmarkValue.hasType( InternalTypeSystem.TYPE_SYSTEM.STRING() ) )
        {
            return InternalBookmark.parse( bookmarkValue.asString() );
        }
        return InternalBookmark.empty();
    }

    public static ServerVersion extractNeo4jServerVersion( Map<String,Value> metadata )
    {
        Value versionValue = metadata.get( "server" );
        if ( versionValue == null || versionValue.isNull() )
        {
            throw new UntrustedServerException( "Server provides no product identifier" );
        }
        else
        {
            ServerVersion server = ServerVersion.version( versionValue.asString() );
            if ( ServerVersion.NEO4J_PRODUCT.equalsIgnoreCase( server.product() ) )
            {
                return server;
            }
            else
            {
                throw new UntrustedServerException( "Server does not identify as a genuine Neo4j instance: '" + server.product() + "'" );
            }
        }
    }

    private static QueryType extractQueryType( Map<String,Value> metadata )
    {
        Value typeValue = metadata.get( "type" );
        if ( typeValue != null )
        {
            return QueryType.fromCode( typeValue.asString() );
        }
        return null;
    }

    private static InternalSummaryCounters extractCounters( Map<String,Value> metadata )
    {
        Value countersValue = metadata.get( "stats" );
        if ( countersValue != null )
        {
            return new InternalSummaryCounters(
                    counterValue( countersValue, "nodes-created" ),
                    counterValue( countersValue, "nodes-deleted" ),
                    counterValue( countersValue, "relationships-created" ),
                    counterValue( countersValue, "relationships-deleted" ),
                    counterValue( countersValue, "properties-set" ),
                    counterValue( countersValue, "labels-added" ),
                    counterValue( countersValue, "labels-removed" ),
                    counterValue( countersValue, "indexes-added" ),
                    counterValue( countersValue, "indexes-removed" ),
                    counterValue( countersValue, "constraints-added" ),
                    counterValue( countersValue, "constraints-removed" ),
                    counterValue( countersValue, "system-updates" )
            );
        }
        return null;
    }

    private static int counterValue( Value countersValue, String name )
    {
        Value value = countersValue.get( name );
        return value.isNull() ? 0 : value.asInt();
    }

    private static Plan extractPlan( Map<String,Value> metadata )
    {
        Value planValue = metadata.get( "plan" );
        if ( planValue != null )
        {
            return InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply( planValue );
        }
        return null;
    }

    private static ProfiledPlan extractProfiledPlan( Map<String,Value> metadata )
    {
        Value profiledPlanValue = metadata.get( "profile" );
        if ( profiledPlanValue != null )
        {
            return InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply( profiledPlanValue );
        }
        return null;
    }

    private static List<Notification> extractNotifications( Map<String,Value> metadata )
    {
        Value notificationsValue = metadata.get( "notifications" );
        if ( notificationsValue != null )
        {
            return notificationsValue.asList( InternalNotification.VALUE_TO_NOTIFICATION );
        }
        return Collections.emptyList();
    }

    private static long extractResultConsumedAfter( Map<String,Value> metadata, String key )
    {
        Value resultConsumedAfterValue = metadata.get( key );
        if ( resultConsumedAfterValue != null )
        {
            return resultConsumedAfterValue.asLong();
        }
        return -1;
    }
}
