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
package org.neo4j.connector.handlers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.neo4j.connector.internal.util.MetadataExtractor;
import org.neo4j.connector.spi.ResponseHandler;
import org.neo4j.connector.Value;

import static java.util.Collections.emptyList;

public class RunResponseHandler implements ResponseHandler
{
    private final CompletableFuture<Throwable> runCompletedFuture;
    private final MetadataExtractor metadataExtractor;
    private long queryId = MetadataExtractor.ABSENT_QUERY_ID;

    private List<String> queryKeys = emptyList();
    private long resultAvailableAfter = -1;

    public RunResponseHandler( MetadataExtractor metadataExtractor )
    {
        this( new CompletableFuture<>(), metadataExtractor );
    }

    public RunResponseHandler( CompletableFuture<Throwable> runCompletedFuture, MetadataExtractor metadataExtractor )
    {
        this.runCompletedFuture = runCompletedFuture;
        this.metadataExtractor = metadataExtractor;
    }

    @Override
    public void onSuccess( Map<String,Value> metadata )
    {
        queryKeys = metadataExtractor.extractQueryKeys( metadata );
        resultAvailableAfter = metadataExtractor.extractResultAvailableAfter( metadata );
        queryId = metadataExtractor.extractQueryId( metadata );

        completeRunFuture( null );
    }

    @Override
    public void onFailure( Throwable error )
    {
        completeRunFuture( error );
    }

    @Override
    public void onRecord( Value[] fields )
    {
        throw new UnsupportedOperationException();
    }

    public List<String> queryKeys()
    {
        return queryKeys;
    }

    public long resultAvailableAfter()
    {
        return resultAvailableAfter;
    }

    public long queryId()
    {
        return queryId;
    }

    /**
     * Complete the given future with error if the future was failed.
     * Future is never completed exceptionally.
     * Async API needs to wait for RUN because it needs to access query keys.
     * Reactive API needs to know if RUN failed by checking the error.
     */
    private void completeRunFuture( Throwable error )
    {
        runCompletedFuture.complete( error );
    }

    public CompletableFuture<Throwable> runFuture()
    {
        return runCompletedFuture;
    }
}
