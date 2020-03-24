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
package org.neo4j.connector.messaging.v4;

import org.neo4j.connector.async.UnmanagedTransaction;
import org.neo4j.connector.handlers.PullAllResponseHandler;
import org.neo4j.connector.handlers.PullHandlers;
import org.neo4j.connector.handlers.RunResponseHandler;
import org.neo4j.connector.handlers.pulln.PullResponseHandler;
import org.neo4j.connector.messaging.v3.BoltProtocolV3;
import org.neo4j.connector.Query;
import org.neo4j.connector.internal.BookmarkHolder;
import org.neo4j.connector.DatabaseName;
import org.neo4j.connector.internal.cursor.ResultCursorFactoryImpl;
import org.neo4j.connector.internal.cursor.ResultCursorFactory;
import org.neo4j.connector.messaging.BoltProtocol;
import org.neo4j.connector.messaging.MessageFormat;
import org.neo4j.connector.messaging.request.RunWithMetadataMessage;
import org.neo4j.connector.spi.Connection;


public class BoltProtocolV4 extends BoltProtocolV3
{
    public static final int VERSION = 4;
    public static final BoltProtocol INSTANCE = new BoltProtocolV4();

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV4();
    }

    @Override
    protected ResultCursorFactory buildResultCursorFactory( Connection connection, Query query, BookmarkHolder bookmarkHolder,
                                                            UnmanagedTransaction tx, RunWithMetadataMessage runMessage, boolean waitForRunResponse, long fetchSize )
    {
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );

        PullAllResponseHandler pullAllHandler = PullHandlers.newBoltV4AutoPullHandler( query, runHandler, connection, bookmarkHolder, tx, fetchSize );
        PullResponseHandler pullHandler = PullHandlers.newBoltV4BasicPullHandler( query, runHandler, connection, bookmarkHolder, tx );

        return new ResultCursorFactoryImpl( connection, runMessage, runHandler, pullHandler, pullAllHandler, waitForRunResponse );
    }

    @Override
    protected void verifyDatabaseNameBeforeTransaction( DatabaseName databaseName )
    {
        // Bolt V4 accepts database name
    }

    @Override
    public int version()
    {
        return VERSION;
    }
}
