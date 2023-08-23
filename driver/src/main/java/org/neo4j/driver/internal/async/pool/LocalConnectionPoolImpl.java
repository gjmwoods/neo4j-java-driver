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
package org.neo4j.driver.internal.async.pool;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.async.connection.ChannelAttributes.setAuthorizationStateListener;
import static org.neo4j.driver.internal.util.Futures.combineErrors;
import static org.neo4j.driver.internal.util.Futures.completeWithNullIfNoError;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLockAsync;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.async.connection.LocalChannelConnector;
import org.neo4j.driver.internal.metrics.MetricsListener;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.LocalConnectionPool;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.net.ServerAddress;

public class LocalConnectionPoolImpl implements LocalConnectionPool {
    private final LocalChannelConnector connector;
    private final Bootstrap bootstrap;
    private final NettyChannelTracker nettyChannelTracker;
    private final Supplier<NettyChannelHealthChecker> channelHealthCheckerSupplier;
    private final PoolSettings settings;
    private final Logger log;
    private final MetricsListener metricsListener;
    private final boolean ownsEventLoopGroup;

    private final ReadWriteLock addressToPoolLock = new ReentrantReadWriteLock();
    private final Map<LocalAddress, ExtendedChannelPool> addressToPool = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final ConnectionFactory connectionFactory;
    private final Clock clock;

    public LocalConnectionPoolImpl(
            LocalChannelConnector connector,
            Bootstrap bootstrap,
            PoolSettings settings,
            MetricsListener metricsListener,
            Logging logging,
            Clock clock,
            boolean ownsEventLoopGroup) {
        this(
                connector,
                bootstrap,
                new NettyChannelTracker(
                        metricsListener, bootstrap.config().group().next(), logging),
                settings,
                metricsListener,
                logging,
                clock,
                ownsEventLoopGroup,
                new NetworkConnectionFactory(clock, metricsListener, logging));
    }

    protected LocalConnectionPoolImpl(
            LocalChannelConnector connector,
            Bootstrap bootstrap,
            NettyChannelTracker nettyChannelTracker,
            PoolSettings settings,
            MetricsListener metricsListener,
            Logging logging,
            Clock clock,
            boolean ownsEventLoopGroup,
            ConnectionFactory connectionFactory) {
        requireNonNull(clock, "clock must not be null");
        this.connector = connector;
        this.bootstrap = bootstrap;
        this.nettyChannelTracker = nettyChannelTracker;
        this.channelHealthCheckerSupplier = () -> new NettyChannelHealthChecker(settings, clock, logging);
        this.settings = settings;
        this.metricsListener = metricsListener;
        this.log = logging.getLog(getClass());
        this.ownsEventLoopGroup = ownsEventLoopGroup;
        this.connectionFactory = connectionFactory;
        this.clock = clock;
    }

    @Override
    public CompletionStage<Connection> acquire(LocalAddress address, AuthToken overrideAuthToken) {
        log.trace("Acquiring a connection from pool towards %s", address);

        assertNotClosed();
        var pool = getOrCreatePool(address);

        var acquireEvent = metricsListener.createListenerEvent();
        metricsListener.beforeAcquiringOrCreating(pool.id(), acquireEvent);
        var channelFuture = pool.acquire(overrideAuthToken);

        return channelFuture.handle((channel, error) -> {
            try {
                processAcquisitionError(pool, address, error);
                assertNotClosed(address, channel, pool);
                setAuthorizationStateListener(channel, pool.healthChecker());
                var connection = connectionFactory.createConnection(channel, pool);

                metricsListener.afterAcquiredOrCreated(pool.id(), acquireEvent);
                return connection;
            } finally {
                metricsListener.afterAcquiringOrCreating(pool.id());
            }
        });
    }

    @Override
    public int inUseConnections(ServerAddress address) {
        // todo stuff the local address in the netty channel tracker
        return nettyChannelTracker.inUseChannelCount(address);
    }

    private int idleConnections(ServerAddress address) {
        return nettyChannelTracker.idleChannelCount(address);
    }

    @Override
    public CompletionStage<Void> close() {
        if (closed.compareAndSet(false, true)) {
            nettyChannelTracker.prepareToCloseChannels();

            executeWithLockAsync(addressToPoolLock.writeLock(), () -> {
                // We can only shutdown event loop group when all netty pools are fully closed,
                // otherwise the netty pools might missing threads (from event loop group) to execute clean ups.
                return closeAllPools().whenComplete((ignored, pollCloseError) -> {
                    addressToPool.clear();
                    if (!ownsEventLoopGroup) {
                        completeWithNullIfNoError(closeFuture, pollCloseError);
                    } else {
                        shutdownEventLoopGroup(pollCloseError);
                    }
                });
            });
        }
        return closeFuture;
    }

    @Override
    public boolean isOpen(LocalAddress address) {
        return executeWithLock(addressToPoolLock.readLock(), () -> addressToPool.containsKey(address));
    }

    @Override
    public String toString() {
        return executeWithLock(
                addressToPoolLock.readLock(), () -> "ConnectionPoolImpl{" + "pools=" + addressToPool + '}');
    }

    private void processAcquisitionError(ExtendedChannelPool pool, LocalAddress serverAddress, Throwable error) {
        var cause = Futures.completionExceptionCause(error);
        if (cause != null) {
            if (cause instanceof TimeoutException) {
                // NettyChannelPool returns future failed with TimeoutException if acquire operation takes more than
                // configured time, translate this exception to a prettier one and re-throw
                metricsListener.afterTimedOutToAcquireOrCreate(pool.id());
                throw new ClientException(
                        "Unable to acquire connection from the pool within configured maximum time of "
                                + settings.connectionAcquisitionTimeout() + "ms");
            } else if (pool.isClosed()) {
                // There is a race condition where a thread tries to acquire a connection while the pool is closed by
                // another concurrent thread.
                // Treat as failed to obtain connection for a direct driver. For a routing driver, this error should be
                // retried.
                throw new ServiceUnavailableException(
                        format("Connection pool for server %s is closed while acquiring a connection.", serverAddress),
                        cause);
            } else {
                // some unknown error happened during connection acquisition, propagate it
                throw new CompletionException(cause);
            }
        }
    }

    private void assertNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException(CONNECTION_POOL_CLOSED_ERROR_MESSAGE);
        }
    }

    private void assertNotClosed(LocalAddress address, Channel channel, ExtendedChannelPool pool) {
        if (closed.get()) {
            pool.release(channel);
            closePoolInBackground(address, pool);
            executeWithLock(addressToPoolLock.writeLock(), () -> addressToPool.remove(address));
            assertNotClosed();
        }
    }

    // for testing only
    ExtendedChannelPool getPool(BoltServerAddress address) {
        return executeWithLock(addressToPoolLock.readLock(), () -> addressToPool.get(address));
    }

    ExtendedChannelPool newPool(LocalAddress address) {
        return new LocalNettyChannelPool(
                address,
                connector,
                bootstrap,
                nettyChannelTracker,
                channelHealthCheckerSupplier.get(),
                settings.connectionAcquisitionTimeout(),
                settings.maxConnectionPoolSize(),
                clock);
    }

    private ExtendedChannelPool getOrCreatePool(LocalAddress address) {
        var existingPool = executeWithLock(addressToPoolLock.readLock(), () -> addressToPool.get(address));
        return existingPool != null
                ? existingPool
                : executeWithLock(addressToPoolLock.writeLock(), () -> {
                    var pool = addressToPool.get(address);
                    if (pool == null) {
                        pool = newPool(address);
                        // before the connection pool is added I can register the metrics for the pool.
                        metricsListener.registerPoolMetrics(
                                pool.id(),
                                ServerAddress.of(
                                        "local-channel", 1), // todo mapping from local address to serveraddress
                                () -> 1,
                                () -> 1);
                        addressToPool.put(address, pool);
                    }
                    return pool;
                });
    }

    private CompletionStage<Void> closePool(ExtendedChannelPool pool) {
        return pool.close()
                .whenComplete((ignored, error) ->
                        // after the connection pool is removed/close, I can remove its metrics.
                        metricsListener.removePoolMetrics(pool.id()));
    }

    private void closePoolInBackground(LocalAddress address, ExtendedChannelPool pool) {
        // Close in the background
        closePool(pool).whenComplete((ignored, error) -> {
            if (error != null) {
                log.warn(format("An error occurred while closing connection pool towards %s.", address), error);
            }
        });
    }

    private EventLoopGroup eventLoopGroup() {
        return bootstrap.config().group();
    }

    private void shutdownEventLoopGroup(Throwable pollCloseError) {
        // This is an attempt to speed up the shut down procedure of the driver
        // This timeout is needed for `closePoolInBackground` to finish background job, especially for races between
        // `acquire` and `close`.
        eventLoopGroup().shutdownGracefully(200, 15_000, TimeUnit.MILLISECONDS);

        Futures.asCompletionStage(eventLoopGroup().terminationFuture())
                .whenComplete((ignore, eventLoopGroupTerminationError) -> {
                    var combinedErrors = combineErrors(pollCloseError, eventLoopGroupTerminationError);
                    completeWithNullIfNoError(closeFuture, combinedErrors);
                });
    }

    private CompletableFuture<Void> closeAllPools() {
        return CompletableFuture.allOf(addressToPool.entrySet().stream()
                .map(entry -> {
                    var address = entry.getKey();
                    var pool = entry.getValue();
                    log.info("Closing connection pool towards %s", address);
                    // Wait for all pools to be closed.
                    return closePool(pool).toCompletableFuture();
                })
                .toArray(CompletableFuture[]::new));
    }
}
