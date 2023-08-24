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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import org.neo4j.driver.internal.BoltServerAddress;

public class LocalDriverToProxyHandler extends ChannelInboundHandlerAdapter {

    private final BoltServerAddress boltServerAddress;
    private Channel remoteChannel;

    public LocalDriverToProxyHandler(BoltServerAddress boltServerAddress) {
        this.boltServerAddress = boltServerAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        final Channel localChannel = ctx.channel();
        final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        // Create a TCP connection to remote neo4j instance.
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new LocalProxyToRemoteHandler(localChannel))
                .option(ChannelOption.AUTO_READ, false);
        ChannelFuture f = b.connect(new InetSocketAddress(boltServerAddress.host(), boltServerAddress.port()));
        remoteChannel = f.channel();
        f.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                localChannel.read();
            } else {
                localChannel.close();
            }
        });
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        // forwarding local bytes to remote neo4j
        if (remoteChannel.isActive()) {
            remoteChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            });
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (remoteChannel != null) {
            LocalToRemoteProxy.flushThenClose(remoteChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LocalToRemoteProxy.flushThenClose(ctx.channel());
    }
}
