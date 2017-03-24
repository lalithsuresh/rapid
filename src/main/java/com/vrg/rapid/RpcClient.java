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

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.ConsensusProposalResponse;
import com.vrg.rapid.pb.GossipMessage;
import com.vrg.rapid.pb.GossipResponse;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.MembershipServiceGrpc.MembershipServiceFutureStub;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.Response;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;

import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * MessagingServiceGrpc client.
 */
final class RpcClient {
    static boolean USE_IN_PROCESS_CHANNEL = false;
    private final HostAndPort address;
    private static final int RPC_TIMEOUT_SECONDS = 1;

    RpcClient(final HostAndPort address) {
        this.address = address;
    }

    ListenableFuture<GossipResponse> sendGossip(final HostAndPort remote,
                                                final GossipMessage gossipMessage) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(gossipMessage);

        // Since gossip is periodic, we will not cache the channel right away.
        final ManagedChannel channel = NettyChannelBuilder
                .forAddress(remote.getHost(), remote.getPort())
                .usePlaintext(true)
                .build();

        return MembershipServiceGrpc.newFutureStub(channel).gossip(gossipMessage);
    }

    ListenableFuture<ProbeResponse> sendProbeMessage(final HostAndPort remote,
                                                     final ProbeMessage probeMessage) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(probeMessage);

        final MembershipServiceFutureStub stub = getFutureStub(remote);
        return stub.receiveProbe(probeMessage);
    }

    ListenableFuture<JoinResponse> sendJoinMessage(final HostAndPort remote,
                                                   final HostAndPort sender, final UUID uuid) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(sender);
        Objects.requireNonNull(uuid);

        final JoinMessage.Builder builder = JoinMessage.newBuilder();
        final JoinMessage msg = builder.setSender(sender.toString())
                .setUuid(uuid.toString())
                .build();
        return sendJoinMessage(remote, msg);
    }

    ListenableFuture<JoinResponse> sendJoinPhase2Message(final HostAndPort remote,
                                                         final HostAndPort sender,
                                                         final UUID uuid,
                                                         final int ringNumber,
                                                         final long configurationId) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(sender);
        Objects.requireNonNull(uuid);

        final JoinMessage.Builder builder = JoinMessage.newBuilder();
        final JoinMessage msg = builder.setSender(sender.toString())
                .setUuid(uuid.toString())
                .setRingNumber(ringNumber)
                .setConfigurationId(configurationId)
                .build();
        final MembershipServiceFutureStub stub = getFutureStub(remote);
        return stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS * 20, TimeUnit.SECONDS).receiveJoinPhase2Message(msg);
    }

    private ListenableFuture<JoinResponse> sendJoinMessage(final HostAndPort remote, final JoinMessage msg) {
        Objects.requireNonNull(msg);
        Objects.requireNonNull(remote);

        final MembershipServiceFutureStub stub = getFutureStub(remote);
        return stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS * 5, TimeUnit.SECONDS).receiveJoinMessage(msg);
    }

    ListenableFuture<ConsensusProposalResponse> sendConsensusProposal(final HostAndPort remote,
                                                                      final ConsensusProposal msg) {
        Objects.requireNonNull(msg);
        final MembershipServiceFutureStub stub = getFutureStub(remote);
        return stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS).receiveConsensusProposal(msg);
    }

    ListenableFuture<Response> sendLinkUpdateMessage(final HostAndPort remote, final BatchedLinkUpdateMessage msg) {
        Objects.requireNonNull(msg);
        final MembershipServiceFutureStub stub = getFutureStub(remote);
        return stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS).receiveLinkUpdateMessage(msg);
    }

    ListenableFuture<Response> sendLinkUpdateMessage(final HostAndPort remote,
                                           final HostAndPort src,
                                           final HostAndPort dst,
                                           final LinkStatus status,
                                           final long configurationId) {
        Objects.requireNonNull(src);
        Objects.requireNonNull(dst);
        Objects.requireNonNull(status);

        final MembershipServiceFutureStub stub = getFutureStub(remote);

        final LinkUpdateMessage msg = LinkUpdateMessage.newBuilder()
                                            .setLinkSrc(src.toString())
                                            .setLinkDst(dst.toString())
                                            .setLinkStatus(status)
                                            .setConfigurationId(configurationId).build();
        final BatchedLinkUpdateMessage batchedMessage = BatchedLinkUpdateMessage.newBuilder()
                                            .setSender(address.toString())
                                            .addAllMessages(Collections.singletonList(msg))
                                            .build();
        return stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS).receiveLinkUpdateMessage(batchedMessage);
    }

    private MembershipServiceFutureStub getFutureStub(final HostAndPort remote) {
        // TODO: allow configuring SSL/TLS
        final Channel channel;

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

        return MembershipServiceGrpc.newFutureStub(channel);
    }
}
