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
package org.neo4j.driver.internal.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.concurrent.TimeUnit;

import org.neo4j.connector.cluster.AddressSet;
import org.neo4j.connector.cluster.RoutingTable;
import org.neo4j.connector.exception.ClientException;
import org.neo4j.connector.internal.BoltServerAddress;
import org.neo4j.connector.summary.ResultSummary;

public final class Matchers
{
    private Matchers()
    {
    }

    public static Matcher<RoutingTable> containsRouter( final BoltServerAddress address )
    {
        return new TypeSafeMatcher<RoutingTable>()
        {
            @Override
            protected boolean matchesSafely( RoutingTable routingTable )
            {
                BoltServerAddress[] addresses = routingTable.routers().toArray();

                for ( BoltServerAddress currentAddress : addresses )
                {
                    if ( currentAddress.equals( address ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "routing table that contains router " ).appendValue( address );
            }
        };
    }

    public static Matcher<RoutingTable> containsReader( final BoltServerAddress address )
    {
        return new TypeSafeMatcher<RoutingTable>()
        {
            @Override
            protected boolean matchesSafely( RoutingTable routingTable )
            {
                return contains( routingTable.readers(), address );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "routing table that contains reader " ).appendValue( address );
            }
        };
    }

    public static Matcher<RoutingTable> containsWriter( final BoltServerAddress address )
    {
        return new TypeSafeMatcher<RoutingTable>()
        {
            @Override
            protected boolean matchesSafely( RoutingTable routingTable )
            {
                return contains( routingTable.writers(), address );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "routing table that contains writer " ).appendValue( address );
            }
        };
    }

    public static Matcher<ResultSummary> containsResultAvailableAfterAndResultConsumedAfter()
    {
        return new TypeSafeMatcher<ResultSummary>()
        {
            @Override
            protected boolean matchesSafely( ResultSummary summary )
            {
                return summary.resultAvailableAfter( TimeUnit.MILLISECONDS ) >= 0L &&
                        summary.resultConsumedAfter( TimeUnit.MILLISECONDS ) >= 0L;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "resultAvailableAfter and resultConsumedAfter " );
            }
        };
    }

    public static Matcher<Throwable> arithmeticError()
    {
        return new TypeSafeMatcher<Throwable>()
        {
            @Override
            protected boolean matchesSafely( Throwable error )
            {
                return error instanceof ClientException &&
                       ((ClientException) error).code().contains( "ArithmeticError" );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "client error with code 'ArithmeticError' " );
            }
        };
    }

    public static Matcher<Throwable> syntaxError( String messagePrefix )
    {
        return new TypeSafeMatcher<Throwable>()
        {
            @Override
            protected boolean matchesSafely( Throwable error )
            {
                if ( error instanceof ClientException )
                {
                    ClientException clientError = (ClientException) error;
                    return clientError.code().contains( "SyntaxError" ) &&
                           clientError.getMessage().startsWith( messagePrefix );
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "client error with code 'SyntaxError' and prefix '" + messagePrefix + "' " );
            }
        };
    }

    public static Matcher<Throwable> connectionAcquisitionTimeoutError( int timeoutMillis )
    {
        return new TypeSafeMatcher<Throwable>()
        {
            @Override
            protected boolean matchesSafely( Throwable error )
            {
                if ( error instanceof ClientException )
                {
                    String expectedMessage = "Unable to acquire connection from the pool within " +
                                             "configured maximum time of " + timeoutMillis + "ms";
                    return expectedMessage.equals( error.getMessage() );
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "acquisition timeout error with " + timeoutMillis + "ms" );
            }
        };
    }

    public static Matcher<Throwable> blockingOperationInEventLoopError()
    {
        return new TypeSafeMatcher<Throwable>()
        {
            @Override
            protected boolean matchesSafely( Throwable error )
            {
                return error instanceof IllegalStateException &&
                       error.getMessage() != null &&
                       error.getMessage().startsWith( "Blocking operation can't be executed in IO thread" );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "IllegalStateException about blocking operation in event loop thread " );
            }
        };
    }

    private static boolean contains( AddressSet set, BoltServerAddress address )
    {
        BoltServerAddress[] addresses = set.toArray();
        for ( BoltServerAddress currentAddress : addresses )
        {
            if ( currentAddress.equals( address ) )
            {
                return true;
            }
        }
        return false;
    }
}
