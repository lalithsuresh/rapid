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
import com.vrg.rapid.pb.Remoting.LinkUpdateMessageWire;
import com.vrg.rapid.pb.Remoting.Response;
import com.vrg.rapid.pb.Remoting.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;


/**
 * MessagingServiceGrpc client.
 */
public class MessagingClient {
    private final ConcurrentHashMap<HostAndPort, MembershipServiceBlockingStub> stubs;

    MessagingClient() {
        stubs = new ConcurrentHashMap<>();
    }

    Response sendLinkUpdateMessage(final HostAndPort src,
                                            final HostAndPort dst,
                                            final Status status,
                                            final long configurationId) {
        Objects.requireNonNull(src);
        Objects.requireNonNull(dst);
        Objects.requireNonNull(status);

        final MembershipServiceBlockingStub stub = stubs.computeIfAbsent(dst, this::createBlockingStub);

        final LinkUpdateMessageWire msg = LinkUpdateMessageWire.newBuilder()
                                            .setSrc(src.toString())
                                            .setDst(dst.toString())
                                            .setStatus(status)
                                            .setConfig(configurationId).build();
        return stub.receiveLinkUpdateMessage(msg);
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
