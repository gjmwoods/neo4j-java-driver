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
package org.neo4j.driver.internal;

import org.neo4j.connector.AbstractQueryRunner;
import org.neo4j.connector.Query;
import org.neo4j.connector.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.connector.async.ResultCursor;
import org.neo4j.connector.async.UnmanagedTransaction;
import org.neo4j.connector.internal.util.Futures;

public class InternalTransaction extends AbstractQueryRunner implements Transaction
{
    private final UnmanagedTransaction tx;
    public InternalTransaction( UnmanagedTransaction tx )
    {
        this.tx = tx;
    }

    @Override
    public void commit()
    {
        Futures.blockingGet( tx.commitAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while committing the transaction" ) );
    }

    @Override
    public void rollback()
    {
        Futures.blockingGet( tx.rollbackAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while rolling back the transaction" ) );
    }

    @Override
    public void close()
    {
        Futures.blockingGet( tx.closeAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the transaction" ) );
    }

    @Override
    public Result run( Query query)
    {
        ResultCursor cursor = Futures.blockingGet( tx.runAsync( query, false ),
                                                   () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in transaction" ) );
        return new InternalResult( tx.connection(), cursor );
    }

    @Override
    public boolean isOpen()
    {
        return tx.isOpen();
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        tx.connection().terminateAndRelease( reason );
    }
}
