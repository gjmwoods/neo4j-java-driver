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
package org.neo4j.driver.internal.async.connection;

import static org.neo4j.driver.internal.async.connection.ChannelConnectorImpl.installChannelConnectedListeners;
import static org.neo4j.driver.internal.async.connection.ChannelConnectorImpl.installHandshakeCompletedListeners;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.local.LocalAddress;
import java.time.Clock;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.cluster.RoutingContext;
import org.neo4j.driver.internal.security.SecurityPlanImpl;

public class EmbeddedChannelConnector implements ChannelConnector {
    private final LocalAddress address = new LocalAddress("BringBackBoris!");
    private final Clock clock;
    private final Logging logging;
    private final String userAgent;
    private final AuthToken authToken;

    public EmbeddedChannelConnector(String userAgent, AuthToken authToken, Clock clock, Logging logging) {
        this.userAgent = userAgent;
        this.authToken = authToken;
        this.clock = clock;
        this.logging = logging;
    }

    @Override
    public ChannelFuture connect(BoltServerAddress ignored, Bootstrap bootstrap) {
        // todo address needed for tracking channels
        bootstrap.handler(new NettyChannelInitializer(null, SecurityPlanImpl.insecure(), 0, clock, logging));

        ChannelFuture channelConnected = bootstrap.connect(address);

        Channel channel = channelConnected.channel();
        ChannelPromise handshakeCompleted = channel.newPromise();
        ChannelPromise connectionInitialized = channel.newPromise();

        installChannelConnectedListeners(
                null, channelConnected, handshakeCompleted, 0, new ChannelPipelineBuilderImpl(), logging);
        installHandshakeCompletedListeners(
                handshakeCompleted, connectionInitialized, userAgent, authToken, RoutingContext.EMPTY);

        return connectionInitialized;
    }
}
