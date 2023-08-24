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
package org.neo4j.driver.testutil.local;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.neo4j.driver.internal.BoltServerAddress;

public final class LocalToRemoteProxy {

    private final LocalAddress localAddress;
    private final BoltServerAddress remoteAddress;

    private EventLoopGroup eventLoopGroup;

    public LocalToRemoteProxy(LocalAddress localAddress, BoltServerAddress remoteAddress) {
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
    }

    public void start() throws Exception {
        // Configure the bootstrap.
        eventLoopGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(eventLoopGroup)
                .channel(LocalServerChannel.class)
                .childHandler(new ChannelInitializer<LocalChannel>() {
                    @Override
                    protected void initChannel(LocalChannel ch) {
                        ch.pipeline().addLast(new LocalDriverToProxyHandler(remoteAddress));
                    }
                })
                .childOption(ChannelOption.AUTO_READ, false)
                .bind(localAddress)
                .sync()
                .channel()
                .closeFuture()
                .sync();
    }

    public void stop() {
        eventLoopGroup.shutdownGracefully();
    }

    static void flushThenClose(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
