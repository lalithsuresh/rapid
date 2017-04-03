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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.ConsensusProposalResponse;
import com.vrg.rapid.pb.GossipMessage;
import com.vrg.rapid.pb.GossipResponse;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.Response;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * gRPC server object. It defers receiving messages until it is ready to
 * host a MembershipService object.
 */
final class RpcServer extends MembershipServiceGrpc.MembershipServiceImplBase {
    static boolean USE_IN_PROCESS_SERVER = false;
    private final HostAndPort address;
    @Nullable private MembershipService membershipService;
    @Nullable private Server server;
    private final ExecutorService executor;


    // Used to queue messages in the RPC layer until we are ready with
    // a MembershipService object
    private final DeferredReceiveInterceptor deferringInterceptor = new DeferredReceiveInterceptor();

    public RpcServer(final HostAndPort address,
                     final ExecutorService executorService) {
        this.address = address;
        this.executor = executorService;
    }

    /**
     * Defined in rapid.proto.
     */
    @Override
    public void gossip(final GossipMessage request,
                       final StreamObserver<GossipResponse> responseObserver) {
        // TODO: unused
        responseObserver.onNext(GossipResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Defined in rapid.proto.
     */
    @Override
    public void receiveLinkUpdateMessage(final BatchedLinkUpdateMessage request,
                                         final StreamObserver<Response> responseObserver) {
        assert membershipService != null;
        membershipService.processLinkUpdateMessage(request);
        responseObserver.onNext(Response.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Defined in rapid.proto.
     */
    @Override
    public void receiveConsensusProposal(final ConsensusProposal request,
                                         final StreamObserver<ConsensusProposalResponse> responseObserver) {
        assert membershipService != null;
        membershipService.processConsensusProposal(request);
        responseObserver.onNext(ConsensusProposalResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Defined in rapid.proto.
     */
    @Override
    public void receiveJoinMessage(final JoinMessage joinMessage,
                                   final StreamObserver<JoinResponse> responseObserver) {
        assert membershipService != null;
        membershipService.processJoinMessage(joinMessage, responseObserver);
    }

    /**
     * Defined in rapid.proto.
     */
    @Override
    public void receiveJoinPhase2Message(final JoinMessage joinMessage,
                                         final StreamObserver<JoinResponse> responseObserver) {
        assert membershipService != null;
        membershipService.processJoinPhaseTwoMessage(joinMessage, responseObserver);
    }

    /**
     * Defined in rapid.proto.
     */
    @Override
    public void receiveProbe(final ProbeMessage probeMessage,
                             final StreamObserver<ProbeResponse> probeResponseObserver) {
        assert membershipService != null;
        membershipService.processProbeMessage(probeMessage, probeResponseObserver);
    }

    /**
     * Invoked by the bootstrap protocol when it has a membership service object
     * ready. Until this method is called, the RpcServer will not have its gRPC service
     * methods invoked.
     *
     * @param service a fully initialized MembershipService object.
     */
    void setMembershipService(final MembershipService service) {
        if (this.membershipService != null) {
            throw new RuntimeException("setMembershipService called more than once");
        }
        this.membershipService = service;
        deferringInterceptor.unblock();
    }

    /**
     * Starts the RPC server.
     *
     * @throws IOException if a server cannot be successfully initialized
     */
    void startServer() throws IOException {
        startServer(Collections.emptyList());
    }

    void startServer(final List<ServerInterceptor> interceptors) throws IOException {
        Objects.requireNonNull(interceptors);
        final ImmutableList.Builder<ServerInterceptor> listBuilder = ImmutableList.builder();
        final List<ServerInterceptor> interceptorList = listBuilder.add(deferringInterceptor)
                                                                   .addAll(interceptors) // called first by grpc
                                                                   .build();
        if (USE_IN_PROCESS_SERVER) {
            final ServerBuilder builder = InProcessServerBuilder.forName(address.toString());
            server = builder.addService(ServerInterceptors
                    .intercept(this, interceptorList))
                    .executor(executor)
                    .build()
                    .start();
        } else {
            final ServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress(address.getHost(),
                                                                                              address.getPort()));
            server = builder.addService(ServerInterceptors
                    .intercept(this, interceptorList))
                    .executor(executor)
                    .build()
                    .start();
        }

        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(this::stopServer));
    }

    /**
     * Shuts down MembershipService and RPC server.
     */
    void stopServer() {
        assert server != null;
        try {
            if (membershipService != null) {
                membershipService.shutdown();
            }
            server.shutdown();
            server.awaitTermination(1, TimeUnit.SECONDS);
            executor.shutdownNow();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
