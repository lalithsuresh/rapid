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
import com.vrg.rapid.pb.Status;
import com.vrg.rapid.pb.LinkUpdateMessageWire;
import com.vrg.rapid.pb.Response;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
    private final List<List<Node>> logProposalList = new ArrayList<>();
    private Server server;

    MembershipService(final HostAndPort myAddr,
                      final int K, final int H, final int L) {
        this.myAddr = Objects.requireNonNull(myAddr);
        this.membershipView = new MembershipView(K, new Node(this.myAddr));
        this.watermarkBuffer = new WatermarkBuffer(K, H, L);
        this.broadcaster = new UnicastToAllBroadcaster(new MessagingClient(myAddr));
        this.logProposals = false;
    }

    MembershipService(final HostAndPort myAddr,
                      final int K, final int H, final int L,
                      final MembershipView membershipView,
                      final boolean logProposals) {
        this.myAddr = Objects.requireNonNull(myAddr);
        this.membershipView = membershipView;
        this.watermarkBuffer = new WatermarkBuffer(K, H, L);
        this.broadcaster = new UnicastToAllBroadcaster(new MessagingClient(myAddr));
        this.logProposals = logProposals;
    }

    void startServer() throws IOException {
        startServer(Collections.emptyList());
    }

    void startServer(final List<ServerInterceptor> interceptors) throws IOException {
        Objects.requireNonNull(interceptors);
        final ServerBuilder builder = ServerBuilder.forPort(myAddr.getPort());
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
                                            request.getLinkStatus());
        receiveLinkUpdateMessage(msg);
        final Response response = Response.newBuilder().build();
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
    private void receiveLinkUpdateMessage(final LinkUpdateMessage msg) {
        Objects.requireNonNull(msg);

        // The invariant we want to maintain is that a node can only go into the
        // membership set once and leave it once.
        if (msg.getStatus().equals(Status.UP) && membershipView.isPresent(new Node(msg.getDst()))) {
            throw new RuntimeException("Received UP message for node already in set");
        }
        if (msg.getStatus().equals(Status.DOWN) && !membershipView.isPresent(new Node(msg.getDst()))) {
            throw new RuntimeException("Received DOWN message for node not in set");
        }

        final List<Node> proposal = proposedViewChange(msg);
        if (proposal.size() != 0) {
            // Initiate proposal
            if (logProposals) {
                logProposalList.add(proposal);
            }

            // Initiate consensus from here.
        }

        // continue gossipping
    }

    void broadcastLinkUpdateMessage(final LinkUpdateMessage msg) {
        final List<HostAndPort> nodes = membershipView.viewRing(0)
                                            .stream()
                                            .map(e -> e.address)
                                            .collect(Collectors.toList());
        broadcaster.broadcast(nodes, msg);
    }

    private List<Node> proposedViewChange(final LinkUpdateMessage msg) {
        return watermarkBuffer.receiveLinkUpdateMessage(msg);
    }

    List<List<Node>> getProposalLog() {
        return Collections.unmodifiableList(logProposalList);
    }

    List<Node> getMembershipView() {
        return membershipView.viewRing(0);
    }
}