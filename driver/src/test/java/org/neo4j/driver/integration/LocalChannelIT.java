/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.integration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.netty.channel.local.LocalAddress;
import java.util.List;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Record;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.testutil.DatabaseExtension;
import org.neo4j.driver.testutil.local.LocalToRemoteProxy;

// @ParallelizableIT
public class LocalChannelIT {
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension();

    private final LocalAddress localAddress = new LocalAddress("GimmeLocalBolt");

    private final LocalToRemoteProxy proxy = new LocalToRemoteProxy(localAddress, neo4j.address());

    @BeforeEach
    void setUp() {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                proxy.start();
            } catch (Exception e) {
                throw new RuntimeException("Local To Remote Proxy failed to start", e);
            }
        });
    }

    @AfterEach
    void tearDown() {
        proxy.stop();
    }

    @Test
    void shouldConnect() throws Exception {
        var driverFactory = new DriverFactory();
        var driver = driverFactory.newInstance(
                localAddress,
                new StaticAuthTokenManager(AuthTokens.basic("neo4j", neo4j.adminPassword())),
                Config.builder().build(),
                null);
        assertDoesNotThrow(driver::verifyConnectivity);

        try (var session = driver.session()) {
            // When I execute a query that yields a result
            List<Record> result = session.run("UNWIND [1,2,3] AS k RETURN k").list();

            // Then the result object should contain the returned values
            assertThat(result.size(), equalTo(3));

            // And it should allow random access
            assertThat(result.get(0).get("k").asLong(), equalTo(1L));
            assertThat(result.get(1).get("k").asLong(), equalTo(2L));
            assertThat(result.get(2).get("k").asLong(), equalTo(3L));

            // And it should allow iteration
            long expected = 0;
            for (Record value : result) {
                expected += 1;
                assertThat(value.get("k"), equalTo(Values.value(expected)));
            }
            assertThat(expected, equalTo(3L));
        }
        driver.close();
    }
}
