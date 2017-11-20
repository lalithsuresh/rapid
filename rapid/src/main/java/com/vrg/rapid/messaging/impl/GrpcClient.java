/*
 * Copyright © 2016 - 2017 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.vrg.rapid.messaging.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.Settings;
import com.vrg.rapid.SharedResources;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.MembershipServiceGrpc.MembershipServiceFutureStub;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.internal.ManagedChannelImpl;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;


/**
 * MessagingServiceGrpc client.
 */
public class GrpcClient implements IMessagingClient {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcClient.class);
    private static final int DEFAULT_BUF_SIZE = 4096;
    public static final boolean DEFAULT_GRPC_USE_IN_PROCESS_TRANSPORT = false;
    public static final int DEFAULT_GRPC_TIMEOUT_MS = 1000;
    public static final int DEFAULT_GRPC_DEFAULT_RETRIES = 5;
    public static final int DEFAULT_GRPC_JOIN_TIMEOUT = DEFAULT_GRPC_TIMEOUT_MS * 5;
    public static final int DEFAULT_GRPC_PROBE_TIMEOUT = 1000;

    private final HostAndPort address;
    private final List<ClientInterceptor> interceptors;
    private final LoadingCache<HostAndPort, Channel> channelMap;
    private final ExecutorService grpcExecutor;
    private final ExecutorService backgroundExecutor;
    @Nullable private final EventLoopGroup eventLoopGroup;
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private final ISettings settings;

    @VisibleForTesting
    public GrpcClient(final HostAndPort address) {
        this(address, Collections.emptyList(), new SharedResources(address), new Settings());
    }

    @VisibleForTesting
    public GrpcClient(final HostAndPort address, final ISettings settings) {
        this(address, Collections.emptyList(), new SharedResources(address), settings);
    }

    public GrpcClient(final HostAndPort address, final List<ClientInterceptor> interceptors,
                      final SharedResources sharedResources, final ISettings settings) {
        this.address = address;
        this.interceptors = interceptors;
        this.settings = settings;
        this.grpcExecutor = sharedResources.getClientChannelExecutor();
        this.backgroundExecutor = sharedResources.getBackgroundExecutor();
        this.eventLoopGroup = settings.getUseInProcessTransport() ? null : sharedResources.getEventLoopGroup();
        final RemovalListener<HostAndPort, Channel> removalListener =
                removal -> shutdownChannel((ManagedChannelImpl) removal.getValue());
        this.channelMap = CacheBuilder.newBuilder()
                .expireAfterAccess(30, TimeUnit.SECONDS)
                .removalListener(RemovalListeners.asynchronous(removalListener, backgroundExecutor))
                .build(new CacheLoader<HostAndPort, Channel>() {
                    @Override
                    public Channel load(final HostAndPort hostAndPort) throws Exception {
                        return getChannel(hostAndPort);
                    }
                });
    }

    /**
     * From IMessagingClient
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessage(final HostAndPort remote, final RapidRequest msg) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(msg);

        final Supplier<ListenableFuture<RapidResponse>> call = () -> {
            final MembershipServiceFutureStub stub = getFutureStub(remote)
                    .withDeadlineAfter(getTimeoutForMessageMs(msg),
                            TimeUnit.MILLISECONDS);
            return stub.sendRequest(msg);
        };
        return callWithRetries(call, remote, settings.getGrpcDefaultRetries());
    }

    /**
     * From IMessagingClient
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessageBestEffort(final HostAndPort remote, final RapidRequest msg) {
        Objects.requireNonNull(msg);
        try {
            return backgroundExecutor.submit(() -> {
                final Supplier<ListenableFuture<RapidResponse>> call = () -> {
                    final MembershipServiceFutureStub stub;
                    stub = getFutureStub(remote).withDeadlineAfter(getTimeoutForMessageMs(msg), TimeUnit.MILLISECONDS);
                    return stub.sendRequest(msg);
                };
                return callWithRetries(call, remote, 0);
            }).get();
        } catch (final InterruptedException | ExecutionException e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    /**
     * Recover resources. For future use in case we provide custom grpcExecutor for the ManagedChannels.
     */
    @Override
    public void shutdown() {
        isShuttingDown.set(true);
        channelMap.invalidateAll();
    }

    /**
     * Takes a call and retries it, returning the result as soon as it completes or the exception
     * caught from the last retry attempt.
     *
     * Adapted from https://github.com/spotify/futures-extra/.../AsyncRetrier.java
     *
     * @param call A supplier of a ListenableFuture, representing the call being retried.
     * @param retries The number of retry attempts to be performed before giving up
     * @param <T> The type of the response.
     * @return Returns a ListenableFuture of type T, that hosts the result of the supplied {@code call}.
     */
    @CanIgnoreReturnValue
    private <T> ListenableFuture<T> callWithRetries(final Supplier<ListenableFuture<T>> call,
                                                    final HostAndPort remote,
                                                    final int retries) {
        final SettableFuture<T> settable = SettableFuture.create();
        startCallWithRetry(call, remote, settable, retries);
        return settable;
    }

    /**
     * Adapted from https://github.com/spotify/futures-extra/.../AsyncRetrier.java
     */
    @SuppressWarnings("checkstyle:illegalcatch")
    private <T> void startCallWithRetry(final Supplier<ListenableFuture<T>> call,
                                        final HostAndPort remote,
                                        final SettableFuture<T> signal,
                                        final int retries) {
        if (isShuttingDown.get() || Thread.currentThread().isInterrupted()) {
            signal.setException(new ShuttingDownException("GrpcClient is shutting down or has been interrupted"));
            return;
        }
        final ListenableFuture<T> callFuture = call.get();
        Futures.addCallback(callFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(@Nullable final T result) {
                signal.set(result);
            }

            @Override
            public void onFailure(final Throwable throwable) {
                LOG.trace("Retrying call {}");
                handleFailure(call, remote, signal, retries, throwable);
            }
        }, backgroundExecutor);
    }

    /**
     * Adapted from https://github.com/spotify/futures-extra/.../AsyncRetrier.java
     */
    private <T> void handleFailure(final Supplier<ListenableFuture<T>> code,
                                   final HostAndPort remote,
                                   final SettableFuture<T> future,
                                   final int retries,
                                   final Throwable t) {
        // GRPC returns an UNAVAILABLE error when the TCP connection breaks and there is no way to recover
        // from it . We therefore shutdown the channel, and subsequent calls will try to re-establish it.
        if (t instanceof StatusRuntimeException
            && ((StatusRuntimeException) t).getStatus().getCode().equals(Status.Code.UNAVAILABLE)) {
            channelMap.invalidate(remote);
        }

        if (retries > 0) {
            startCallWithRetry(code, remote, future, retries - 1);
        } else {
            future.setException(t);
        }
    }

    private MembershipServiceFutureStub getFutureStub(final HostAndPort remote) {
        if (isShuttingDown.get()) {
            throw new ShuttingDownException("GrpcClient is shutting down");
        }
        final Channel channel = channelMap.getUnchecked(remote);
        return MembershipServiceGrpc.newFutureStub(channel);
    }

    private void shutdownChannel(final ManagedChannelImpl channel) {
        channel.shutdown();
    }

    private Channel getChannel(final HostAndPort remote) {
        // TODO: allow configuring SSL/TLS
        Channel channel;
        LOG.debug("Creating channel from {} to {}", address, remote);

        if (settings.getUseInProcessTransport()) {
            channel = InProcessChannelBuilder
                    .forName(remote.toString())
                    .executor(grpcExecutor)
                    .intercept(interceptors)
                    .usePlaintext(true)
                    .idleTimeout(10, TimeUnit.SECONDS)
                    .build();
        } else {
            channel = NettyChannelBuilder
                    .forAddress(remote.getHost(), remote.getPort())
                    .executor(grpcExecutor)
                    .intercept(interceptors)
                    .eventLoopGroup(eventLoopGroup)
                    .usePlaintext(true)
                    .idleTimeout(10, TimeUnit.SECONDS)
                    .withOption(ChannelOption.SO_REUSEADDR, true)
                    .withOption(ChannelOption.SO_SNDBUF, DEFAULT_BUF_SIZE)
                    .withOption(ChannelOption.SO_RCVBUF, DEFAULT_BUF_SIZE)
                    .build();
        }

        return channel;
    }

    /**
     * TODO: These timeouts should be on the Rapid side of the IMessagingClient API.
     *
     * @param msg RapidRequest
     * @return timeout to use for the RapidRequest message
     */
    private int getTimeoutForMessageMs(final RapidRequest msg) {
        switch (msg.getContentCase()) {
            case PROBEMESSAGE:
                return settings.getGrpcProbeTimeoutMs();
            case JOINMESSAGE:
                return settings.getGrpcJoinTimeoutMs();
            default:
                return settings.getGrpcTimeoutMs();
        }
    }

    public interface ISettings {
        boolean getUseInProcessTransport();

        int getGrpcTimeoutMs();

        int getGrpcDefaultRetries();

        int getGrpcJoinTimeoutMs();

        int getGrpcProbeTimeoutMs();
    }

    public static class ShuttingDownException extends RuntimeException {
        ShuttingDownException(final String msg) {
            super(msg);
        }
    }
}
