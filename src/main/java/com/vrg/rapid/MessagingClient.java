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
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.MembershipServiceGrpc.MembershipServiceBlockingStub;
import com.vrg.rapid.pb.LinkUpdateMessageWire;
import com.vrg.rapid.pb.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * MessagingServiceGrpc client.
 */
class MessagingClient {
    private final ConcurrentHashMap<HostAndPort, MembershipServiceBlockingStub> stubs;
    private final HostAndPort address;

    MessagingClient(final HostAndPort address) {
        stubs = new ConcurrentHashMap<>();
        this.address = address;
    }

    JoinResponse sendJoinMessage(final HostAndPort remote, final HostAndPort sender, final UUID uuid) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(sender);
        Objects.requireNonNull(uuid);

        final JoinMessage.Builder builder = JoinMessage.newBuilder();
        final JoinMessage msg = builder.setSender(sender.toString())
                .setSenderUuid(uuid.toString())
                .build();
        return sendJoinMessage(remote, msg);
    }


    JoinResponse sendJoinPhase2Message(final HostAndPort remote,
                                       final HostAndPort sender,
                                       final UUID uuid,
                                       final long configurationId) {
        Objects.requireNonNull(remote);
        Objects.requireNonNull(sender);
        Objects.requireNonNull(uuid);

        final JoinMessage.Builder builder = JoinMessage.newBuilder();
        final JoinMessage msg = builder.setSender(sender.toString())
                .setSenderUuid(uuid.toString())
                .setConfigurationId(configurationId)
                .build();
        final MembershipServiceBlockingStub stub = stubs.computeIfAbsent(remote, this::createBlockingStub);
        return stub.withDeadlineAfter(1, TimeUnit.SECONDS).receiveJoinPhase2Message(msg);
    }

    JoinResponse sendJoinMessage(final HostAndPort remote, final JoinMessage msg) {
        Objects.requireNonNull(msg);
        Objects.requireNonNull(remote);

        final MembershipServiceBlockingStub stub = stubs.computeIfAbsent(remote, this::createBlockingStub);
        return stub.withDeadlineAfter(1, TimeUnit.SECONDS).receiveJoinMessage(msg);
    }

    Response sendLinkUpdateMessage(final HostAndPort remote, final LinkUpdateMessageWire msg) {
        Objects.requireNonNull(msg);
        final MembershipServiceBlockingStub stub = stubs.computeIfAbsent(remote, this::createBlockingStub);
        return stub.withDeadlineAfter(1, TimeUnit.SECONDS).receiveLinkUpdateMessage(msg);
    }

    Response sendLinkUpdateMessage(final HostAndPort remote,
                                   final HostAndPort src,
                                   final HostAndPort dst,
                                   final LinkStatus status,
                                   final long configurationId) {
        Objects.requireNonNull(src);
        Objects.requireNonNull(dst);
        Objects.requireNonNull(status);

        final MembershipServiceBlockingStub stub = stubs.computeIfAbsent(remote, this::createBlockingStub);

        final LinkUpdateMessageWire msg = LinkUpdateMessageWire.newBuilder()
                                            .setSender(address.toString())
                                            .setLinkSrc(src.toString())
                                            .setLinkDst(dst.toString())
                                            .setLinkStatus(status)
                                            .setConfigurationId(configurationId).build();
        return stub.withDeadlineAfter(1, TimeUnit.SECONDS).receiveLinkUpdateMessage(msg);
    }

    private MembershipServiceBlockingStub createBlockingStub(final HostAndPort remote) {
        // TODO: allow configuring SSL/TLS
        final ManagedChannel channel = NettyChannelBuilder
                                        .forAddress(remote.getHostText(), remote.getPort())
                                        .usePlaintext(true)
                                        .build();

        return MembershipServiceGrpc.newBlockingStub(channel);
    }
}
