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
package org.neo4j.driver.stress;

import org.hamcrest.Matchers;
import reactor.core.publisher.Flux;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.connector.internal.util.Futures;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.reactive.RxTransaction;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

public class RxFailingQueryInTx<C extends AbstractContext> extends AbstractRxQuery<C>
{
    public RxFailingQueryInTx( Driver driver )
    {
        super( driver, false );
    }

    @Override
    public CompletionStage<Void> execute( C context )
    {
        CompletableFuture<Void> queryFinished = new CompletableFuture<>();
        RxSession session = newSession( AccessMode.READ, context );
        Flux.usingWhen( session.beginTransaction(),
                tx -> tx.run( "UNWIND [10, 5, 0] AS x RETURN 10 / x" ).records(),
                RxTransaction::commit, ( tx, error ) -> tx.rollback(), null )
                .subscribe( record -> {
                    assertThat( record.get( 0 ).asInt(), either( equalTo( 1 ) ).or( equalTo( 2 ) ) );
                    queryFinished.complete( null );
                }, error -> {
                    Throwable cause = Futures.completionExceptionCause( error );
                    assertThat( cause, Matchers.is( org.neo4j.driver.internal.util.Matchers.arithmeticError() ) );
                    queryFinished.complete( null );
                });
        return queryFinished;
    }
}
