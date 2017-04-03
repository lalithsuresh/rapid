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

package com.vrg.rapid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.ConsensusProposalResponse;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.MembershipServiceGrpc.MembershipServiceFutureStub;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.Response;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


/**
 * MessagingServiceGrpc client.
 */
final class RpcClient {
    private static final Logger LOG = LoggerFactory.getLogger(RpcClient.class);
    static boolean USE_IN_PROCESS_CHANNEL = false;
    private final HostAndPort address;
    private final List<ClientInterceptor> interceptors;
    private final Map<HostAndPort, MembershipServiceFutureStub> channelMap = new ConcurrentHashMap<>();

    RpcClient(final HostAndPort address) {
        this(address, Collections.emptyList());
    }

    RpcClient(final HostAndPort address, final List<ClientInterceptor> interceptors) {
        this.address = address;
        this.interceptors = interceptors;
    }

    /**
     * Send a protobuf ProbeMessage to a remote host.
     *
     * @param remote Remote host to send the message to
     * @param probeMessage Probing message for the remote node's failure detector module.
     * @return A future that returns a ProbeResponse if the call was successful.
     */
    ListenableFuture<ProbeResponse> sendProbeMessage(final HostAndPort remote,
                                                     final ProbeMessage probeMessage) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(probeMessage);

        final MembershipServiceFutureStub stub = channelMap.computeIfAbsent(remote, this::getFutureStub);
        return stub.withDeadlineAfter(Conf.RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS).receiveProbe(probeMessage);
    }

    /**
     * Create and send a protobuf JoinMessage to a remote host.
     *
     * @param remote Remote host to send the message to
     * @param sender The node sending the join message
     * @return A future that returns a JoinResponse if the call was successful.
     */
    ListenableFuture<JoinResponse> sendJoinMessage(final HostAndPort remote,
                                                   final HostAndPort sender,
                                                   final UUID uuid) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(sender);
        Objects.requireNonNull(uuid);

        final JoinMessage.Builder builder = JoinMessage.newBuilder();
        final JoinMessage msg = builder.setSender(sender.toString())
                .setUuid(uuid.toString())
                .build();
        final Supplier<ListenableFuture<JoinResponse>> call = () -> {
            final MembershipServiceFutureStub stub = channelMap.computeIfAbsent(remote, this::getFutureStub)
                    .withDeadlineAfter(Conf.RPC_TIMEOUT_MS * 5,
                            TimeUnit.MILLISECONDS);
            return stub.receiveJoinMessage(msg);
        };
        return callWithRetries(call, Conf.RPC_DEFAULT_RETRIES);
    }

    /**
     * Create and send a protobuf JoinPhase2Message to a remote host.
     *
     * @param remote Remote host to send the message to. This node is expected to initiate LinkUpdate-UP messages.
     * @param msg The JoinMessage for phase two.
     * @return A future that returns a JoinResponse if the call was successful.
     */
    ListenableFuture<JoinResponse> sendJoinPhase2Message(final HostAndPort remote,
                                                         final JoinMessage msg) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(msg);

        final Supplier<ListenableFuture<JoinResponse>> call = () -> {
            final MembershipServiceFutureStub stub = channelMap.computeIfAbsent(remote, this::getFutureStub)
                    .withDeadlineAfter(Conf.RPC_JOIN_PHASE_2_TIMEOUT,
                            TimeUnit.MILLISECONDS);
            return stub.receiveJoinPhase2Message(msg);
        };
        return callWithRetries(call, Conf.RPC_DEFAULT_RETRIES);
    }

    /**
     * Sends a consensus proposal to a remote node
     *
     * @param remote Remote host to send the message to.
     * @param msg Consensus proposal message
     * @return A future that returns ConsensusProposalResponse if the message was successful.
     */
    ListenableFuture<ConsensusProposalResponse> sendConsensusProposal(final HostAndPort remote,
                                                                      final ConsensusProposal msg) {
        Objects.requireNonNull(msg);

        final Supplier<ListenableFuture<ConsensusProposalResponse>> call = () -> {
            final MembershipServiceFutureStub stub = channelMap.computeIfAbsent(remote, this::getFutureStub)
                    .withDeadlineAfter(Conf.RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return stub.receiveConsensusProposal(msg);
        };
        return callWithRetries(call, Conf.RPC_DEFAULT_RETRIES);
    }

    /**
     * Sends a link update message to a remote node
     *
     * @param remote Remote host to send the message to.
     * @param msg A BatchedLinkUpdateMessage that contains one or more LinkUpdateMessages
     * @return A future that returns Response if the message was successful.
     */
    ListenableFuture<Response> sendLinkUpdateMessage(final HostAndPort remote, final BatchedLinkUpdateMessage msg) {
        Objects.requireNonNull(msg);
        final Supplier<ListenableFuture<Response>> call = () -> {
            final MembershipServiceFutureStub stub = channelMap.computeIfAbsent(remote, this::getFutureStub)
                    .withDeadlineAfter(Conf.RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return stub.receiveLinkUpdateMessage(msg);
        };
        return callWithRetries(call, Conf.RPC_DEFAULT_RETRIES);
    }

    /**
     * Clear all existing cached stubs and prepare new ones for a set of nodes.
     *
     * @param nodeSet set of nodes to prepare a long lived connection for
     */
    void updateLongLivedConnections(final Set<HostAndPort> nodeSet) {
        //  TODO: We need smarter policies to clear out channels we don't need.
    }

    /**
     * Create new long-lived channels for a set of nodes.
     *
     * @param nodeSet set of nodes to prepare a long lived connection for
     */
    void createLongLivedConnections(final Set<HostAndPort> nodeSet) {
    }

    /**
     * Recover resources. For future use in case we provide custom executors for the ManagedChannels.
     */
    void shutdown() {
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
    private static <T> ListenableFuture<T> callWithRetries(final Supplier<ListenableFuture<T>> call,
                                                           final int retries) {
        final SettableFuture<T> settable = SettableFuture.create();
        startCallWithRetry(call, settable, retries);
        return settable;
    }

    /**
     * Adapted from https://github.com/spotify/futures-extra/.../AsyncRetrier.java
     */
    private static <T> void startCallWithRetry(final Supplier<ListenableFuture<T>> call,
                                               final SettableFuture<T> signal,
                                               final int retries) {
        ListenableFuture<T> callFuture;
        try {
            callFuture = call.get();
        } catch (final StatusRuntimeException e) {
            handleFailure(call, signal, retries, e);
            return;
        }

        Futures.addCallback(callFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(@Nullable final T result) {
                signal.set(result);
            }

            @Override
            public void onFailure(final Throwable throwable) {
                LOG.trace("Retrying call {}");
                handleFailure(call, signal, retries, throwable);
            }
        });
    }

    /**
     * Adapted from https://github.com/spotify/futures-extra/.../AsyncRetrier.java
     */
    private static <T> void handleFailure(final Supplier<ListenableFuture<T>> code,
                                          final SettableFuture<T> future,
                                          final int retries,
                                          final Throwable t) {
        if (retries > 0) {
            startCallWithRetry(code, future, retries - 1);
        } else {
            future.setException(t);
        }
    }

    private MembershipServiceFutureStub getFutureStub(final HostAndPort remote) {
        // TODO: allow configuring SSL/TLS
        Channel channel;
        LOG.debug("Creating channel from {} to {}", address, remote);

        if (channelMap.containsKey(remote)) {
            return channelMap.get(remote);
        }

        if (USE_IN_PROCESS_CHANNEL) {
            channel = InProcessChannelBuilder
                    .forName(remote.toString())
                    .usePlaintext(true)
                    .build();
        } else {
            channel = NettyChannelBuilder
                    .forAddress(remote.getHost(), remote.getPort())
                    .usePlaintext(true)
                    .build();
        }

        if (interceptors.size() > 0) {
            channel = ClientInterceptors.intercept(channel, interceptors);
        }

        return MembershipServiceGrpc.newFutureStub(channel);
    }

    @VisibleForTesting
    static class Conf {
        static int RPC_TIMEOUT_MEDIUM_MS = 1000;
        static int RPC_TIMEOUT_MS = RPC_TIMEOUT_MEDIUM_MS;
        static int RPC_DEFAULT_RETRIES = 5;
        static int RPC_JOIN_PHASE_2_TIMEOUT = 20000;
    }
}
