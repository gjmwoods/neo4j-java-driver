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
package org.neo4j.connector.async;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AbstractQueryRunner;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

//todo not sure about this class. Do query runners form part of the connector API?
public abstract class AsyncAbstractQueryRunner implements AsyncQueryRunner
{
    @Override
    public final CompletionStage<ResultCursor> runAsync(String query, Value parameters )
    {
        return runAsync( new Query(query, parameters ) );
    }

    @Override
    public final CompletionStage<ResultCursor> runAsync(String query, Map<String,Object> parameters)
    {
        return runAsync( query, AbstractQueryRunner.parameters( parameters) );
    }

    @Override
    public final CompletionStage<ResultCursor> runAsync(String query, Record parameters)
    {
        return runAsync( query, AbstractQueryRunner.parameters( parameters) );
    }

    @Override
    public final CompletionStage<ResultCursor> runAsync(String query)
    {
        return runAsync(query, Values.EmptyMap );
    }
}
