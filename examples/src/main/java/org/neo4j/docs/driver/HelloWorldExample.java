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
package org.neo4j.docs.driver;

// tag::hello-world-import[]

import java.io.File;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import static org.neo4j.driver.Values.parameters;
// end::hello-world-import[]

// tag::hello-world[]
public class HelloWorldExample implements AutoCloseable
{
    private final Driver driver;

    public HelloWorldExample( String uri, String user, String password )
    {
        Config customCert = Config.builder()
                                  .withEncryption()
                                  .withTrustStrategy( Config.TrustStrategy
                                                              .trustCustomCertificateSignedBy( new File( "/Users/gregwoods/Documents/GlobalSign.cer") )
                                                              //.trustCustomCertificateSignedBy( new File( "/Users/gregwoods/Documents/AnotherCert.cer") )
                                                              .trustSystemCertificates()
                                                              .withCertificateRevocationCheck()
                                                              .withoutHostnameVerification()).build();

driver = GraphDatabase.driver( "bolt://digicert.com:443", AuthTokens.basic( user, password ), customCert );
        //driver = GraphDatabase.driver( "bolt://google.com:443", AuthTokens.basic( user, password ), customCert );
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public void printGreeting( final String message )
    {
        try ( Session session = driver.session() )
        {
            String greeting = session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute( Transaction tx )
                {
                    Result result = tx.run( "CREATE (a:Greeting) " +
                                                     "SET a.message = $message " +
                                                     "RETURN a.message + ', from node ' + id(a)",
                            parameters( "message", message ) );
                    return result.single().get( 0 ).asString();
                }
            } );
            System.out.println( greeting );
        }
    }

    public static void main( String... args ) throws Exception
    {
        try ( HelloWorldExample greeter = new HelloWorldExample( "bolt://localhost:7687", "neo4j", "password" ) )
        {
            greeter.printGreeting( "hello, world" );
        }
    }
}
// end::hello-world[]

// tag::hello-world-output[]
// hello, world, from node 1234
// end::hello-world-output[]
