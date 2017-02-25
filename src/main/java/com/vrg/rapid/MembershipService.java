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
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessageWire;
import com.vrg.rapid.pb.Response;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;


/**
 * Created by MembershipService
 */
public class MembershipService extends MembershipServiceGrpc.MembershipServiceImplBase {
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final HostAndPort myAddr;
    private final IBroadcaster broadcaster;
    private final boolean logProposals;
    private final List<List<HostAndPort>> logProposalList = new ArrayList<>();
    private Server server;

    public static class Builder {
        private final MembershipView membershipView;
        private final WatermarkBuffer watermarkBuffer;
        private final HostAndPort myAddr;
        private IBroadcaster broadcaster;
        private boolean logProposals;

        public Builder(final HostAndPort myAddr,
                       final WatermarkBuffer watermarkBuffer,
                       final MembershipView membershipView) {
            this.myAddr = Objects.requireNonNull(myAddr);
            this.watermarkBuffer = Objects.requireNonNull(watermarkBuffer);
            this.membershipView = Objects.requireNonNull(membershipView);
            this.broadcaster = new UnicastToAllBroadcaster(new MessagingClient(myAddr));
        }

        public Builder setBroadcaster(final IBroadcaster broadcaster) {
            this.broadcaster = broadcaster;
            return this;
        }

        public Builder setLogProposals(final boolean logProposals) {
            this.logProposals = logProposals;
            return this;
        }

        public MembershipService build() {
            return new MembershipService(this);
        }
    }

    private MembershipService(final Builder builder) {
        this.myAddr = builder.myAddr;
        this.membershipView = builder.membershipView;
        this.watermarkBuffer = builder.watermarkBuffer;
        this.broadcaster = builder.broadcaster;
        this.logProposals = builder.logProposals;
    }

    void startServer() throws IOException {
        startServer(Collections.emptyList());
    }

    void startServer(final List<ServerInterceptor> interceptors) throws IOException {
        Objects.requireNonNull(interceptors);
        final ServerBuilder builder = NettyServerBuilder.forPort(myAddr.getPort());
        server = builder.addService(ServerInterceptors
                                   .intercept(this, interceptors))
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            this.stopServer();
            System.err.println("*** server shut down");
        }));
    }

    void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Override
    public void receiveLinkUpdateMessage(final LinkUpdateMessageWire request,
                                         final StreamObserver<Response> responseObserver) {
        final LinkUpdateMessage msg = new LinkUpdateMessage(request.getLinkSrc(), request.getLinkDst(),
                                            request.getLinkStatus(), request.getConfigurationId());
        processLinkUpdateMessage(msg);
        final Response response = Response.newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void receiveJoinMessage(final JoinMessage joinMessage,
                                   final StreamObserver<JoinResponse> responseObserver) {
        final JoinResponse response = processJoinMessage(joinMessage);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * This method receives link update events and delivers them to
     * the watermark buffer to check if it will return a valid
     * proposal.
     *
     * Link update messages that do not affect an ongoing proposal
     * needs to be dropped.
     */
    private void processLinkUpdateMessage(final LinkUpdateMessage msg) {
        Objects.requireNonNull(msg);

        final long currentConfigurationId = membershipView.getCurrentConfigurationId();
        if (currentConfigurationId != msg.getConfigurationId()) {
            throw new RuntimeException("Configuration ID mismatch: {incoming: " +
                    msg.getConfigurationId() + ", local:" + currentConfigurationId + "}");
        }

        // The invariant we want to maintain is that a node can only go into the
        // membership set once and leave it once.
        if (msg.getStatus().equals(LinkStatus.UP) && membershipView.isPresent(msg.getDst())) {
            throw new RuntimeException("Received UP message for node already in set");
        }
        if (msg.getStatus().equals(LinkStatus.DOWN) && !membershipView.isPresent(msg.getDst())) {
            throw new RuntimeException("Received DOWN message for node not in set");
        }

        final List<HostAndPort> proposal = proposedViewChange(msg);
        if (proposal.size() != 0) {
            // Initiate proposal
            if (logProposals) {
                logProposalList.add(proposal);
            }

            // Initiate consensus from here.
        }

        // continue gossipping
    }

    private JoinResponse processJoinMessage(final JoinMessage joinMessage) {
        final HostAndPort joiningHost = HostAndPort.fromString(joinMessage.getSender());
        final UUID uuid = UUID.fromString(joinMessage.getSenderUuid());
        final JoinStatusCode statusCode = membershipView.isSafeToJoin(joiningHost, uuid);
        final JoinResponse.Builder builder = JoinResponse.newBuilder()
                                                   .setSender(this.myAddr.toString())
                                                   .setStatusCode(statusCode);
        if (statusCode.equals(JoinStatusCode.SAFE_TO_JOIN)) {
            // Return the list of IDs and hosts to the joining node so that it
            // is ready to be part of the new configuration.
            final MembershipView.Configuration configuration = membershipView.getConfiguration();
            builder.addAllIdentifiers(configuration.uuids
                                        .stream()
                                        .map(UUID::toString)
                                        .collect(Collectors.toList()))
                   .addAllHosts(configuration.hostAndPorts
                                .stream()
                                .map(HostAndPort::toString)
                                .collect(Collectors.toList()));
        }

        return builder.build();
    }

    void broadcastLinkUpdateMessage(final LinkUpdateMessage msg) {
        final List<HostAndPort> nodes = membershipView.viewRing(0);
        broadcaster.broadcast(nodes, msg);
    }

    private List<HostAndPort> proposedViewChange(final LinkUpdateMessage msg) {
        return watermarkBuffer.aggregateForProposal(msg);
    }

    List<List<HostAndPort>> getProposalLog() {
        return Collections.unmodifiableList(logProposalList);
    }

    List<HostAndPort> getMembershipView() {
        return membershipView.viewRing(0);
    }
}