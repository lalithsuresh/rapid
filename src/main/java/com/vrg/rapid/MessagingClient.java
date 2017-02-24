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
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.MembershipServiceGrpc.MembershipServiceBlockingStub;
import com.vrg.rapid.pb.LinkUpdateMessageWire;
import com.vrg.rapid.pb.Response;
import com.vrg.rapid.pb.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Objects;
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

    Response sendLinkUpdateMessage(final HostAndPort remote, final LinkUpdateMessage msg) {
        Objects.requireNonNull(msg);
        return sendLinkUpdateMessage(remote, msg.getSrc(), msg.getDst(), msg.getStatus(), msg.getConfigurationId());
    }

    Response sendLinkUpdateMessage(final HostAndPort remote,
                                   final HostAndPort src,
                                   final HostAndPort dst,
                                   final Status status,
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
        final ManagedChannel channel = ManagedChannelBuilder
                                        .forAddress(remote.getHostText(), remote.getPort())
                                        .usePlaintext(true)
                                        .build();

        return MembershipServiceGrpc.newBlockingStub(channel);
    }
}
