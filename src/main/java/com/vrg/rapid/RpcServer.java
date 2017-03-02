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
import com.vrg.rapid.pb.GossipMessage;
import com.vrg.rapid.pb.GossipResponse;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.LinkUpdateMessageWire;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.Response;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * gRPC server object.
 */
class RpcServer extends MembershipServiceGrpc.MembershipServiceImplBase {
    static boolean USE_IN_PROCESS_SERVER = false;
    private final HostAndPort address;
    private final BlockingQueue<Runnable> deferredMessages = new LinkedBlockingDeque<>();
    @Nullable private MembershipService membershipService;
    @Nullable private Server server;

    public RpcServer(final HostAndPort address) {
        this.address = address;
    }

    /**
     * rpc implementations for methods defined in rapid.proto.
     */
    @Override
    public void gossip(final GossipMessage request,
                       final StreamObserver<GossipResponse> responseObserver) {
        // TODO: unused
        responseObserver.onNext(GossipResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void receiveLinkUpdateMessage(final LinkUpdateMessageWire request,
                                         final StreamObserver<Response> responseObserver) {
        final Runnable runnable = () -> {
            assert membershipService != null;
            membershipService.processLinkUpdateMessage(request);
            responseObserver.onNext(Response.getDefaultInstance());
            responseObserver.onCompleted();
        };
        if (membershipService == null) {
            deferredMessages.add(runnable);
        }
        else {
            runnable.run();
        }
    }

    @Override
    public void receiveJoinMessage(final JoinMessage joinMessage,
                                   final StreamObserver<JoinResponse> responseObserver) {
        final Runnable runnable = () -> {
            assert membershipService != null;
            final JoinResponse response = membershipService.processJoinMessage(joinMessage);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        };
        if (membershipService == null) {
            deferredMessages.add(runnable);
        }
        else {
            runnable.run();
        }
    }

    @Override
    public void receiveJoinPhase2Message(final JoinMessage joinMessage,
                                         final StreamObserver<JoinResponse> responseObserver) {
        final Runnable runnable = () -> {
            assert membershipService != null;
            membershipService.processJoinPhaseTwoMessage(joinMessage, responseObserver);
        };
        if (membershipService == null) {
            deferredMessages.add(runnable);
        }
        else {
            runnable.run();
        }
    }

    void setMembershipService(final MembershipService service) {
        this.membershipService = service;
        deferredMessages.forEach(runnable -> {
            try {
                runnable.run();
            }
            catch (final StatusRuntimeException e) {
                // we need to log this
                System.err.println("Dropping runnable with status " + e.getStatus());
            }
        });
    }

    void startServer() throws IOException {
        startServer(Collections.emptyList());
    }

    void startServer(final List<ServerInterceptor> interceptors) throws IOException {
        Objects.requireNonNull(interceptors);

        if (USE_IN_PROCESS_SERVER) {
            final ServerBuilder builder = InProcessServerBuilder.forName(address.toString());
            server = builder.addService(ServerInterceptors
                    .intercept(this, interceptors))
                    .build()
                    .start();
        } else {
            final ServerBuilder builder = NettyServerBuilder.forPort(address.getPort());
            server = builder.addService(ServerInterceptors
                    .intercept(this, interceptors))
                    .build()
                    .start();
        }

        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(this::stopServer));
    }

    void stopServer() {
        assert server != null;
        server.shutdown();
    }

    void blockUntilShutdown() throws InterruptedException {
        assert server != null;
        server.awaitTermination();
    }
}
