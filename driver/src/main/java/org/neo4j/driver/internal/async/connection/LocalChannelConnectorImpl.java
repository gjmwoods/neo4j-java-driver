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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.local.LocalAddress;
import java.time.Clock;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.Config;
import org.neo4j.driver.Logging;
import org.neo4j.driver.NotificationConfig;
import org.neo4j.driver.internal.BoltAgent;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.ConnectionSettings;
import org.neo4j.driver.internal.cluster.RoutingContext;

public class LocalChannelConnectorImpl implements LocalChannelConnector {
    private final Clock clock;
    private final Logging logging;
    private final String userAgent;
    private final BoltAgent boltAgent;
    private final AuthTokenManager authTokenManager;
    private final NotificationConfig notificationConfig;

    private final ChannelPipelineBuilder channelPipelineBuilder;

    public LocalChannelConnectorImpl(
            ConnectionSettings connectionSettings, BoltAgent boltAgent, Config config, Clock clock) {
        this.userAgent = connectionSettings.userAgent();
        this.clock = clock;
        this.logging = config.logging();
        this.authTokenManager = connectionSettings.authTokenProvider();
        this.boltAgent = boltAgent;
        this.notificationConfig = config.notificationConfig();
        this.channelPipelineBuilder = new ChannelPipelineBuilderImpl();
    }

    @Override
    public ChannelFuture connect(LocalAddress localAddress, Bootstrap bootstrap) {
        bootstrap.handler(new LocalNettyChannelInitializer(authTokenManager, clock, logging));

        ChannelFuture channelConnected = bootstrap.connect(localAddress);

        Channel channel = channelConnected.channel();
        ChannelPromise handshakeCompleted = channel.newPromise();
        ChannelPromise connectionInitialized = channel.newPromise();

        installChannelConnectedListeners(BoltServerAddress.LOCAL_ADDRESS_MARKER, channelConnected, handshakeCompleted);
        installHandshakeCompletedListeners(handshakeCompleted, connectionInitialized);

        return connectionInitialized;
    }

    public void installChannelConnectedListeners(
            BoltServerAddress address, ChannelFuture channelConnected, ChannelPromise handshakeCompleted) {
        // add listener that sends Bolt handshake bytes when channel is connected
        channelConnected.addListener(
                new ChannelConnectedListener(address, channelPipelineBuilder, handshakeCompleted, logging));
    }

    private void installHandshakeCompletedListeners(
            ChannelPromise handshakeCompleted, ChannelPromise connectionInitialized) {
        handshakeCompleted.addListener(new HandshakeCompletedListener(
                userAgent, boltAgent, RoutingContext.EMPTY, connectionInitialized, notificationConfig, clock));
    }
}
