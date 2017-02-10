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
import com.vrg.rapid.pb.Remoting;
import com.vrg.rapid.pb.Remoting.LinkUpdateMessageWire;
import com.vrg.rapid.pb.Remoting.Response;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.Objects;


/**
 * Created by MembershipService
 */
public class MembershipService extends MembershipServiceGrpc.MembershipServiceImplBase {
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final HostAndPort myAddr;
    private Server server;

    MembershipService(final HostAndPort myAddr,
                             final int K, final int H, final int L) {
        this.myAddr = Objects.requireNonNull(myAddr);
        this.membershipView = new MembershipView(K, new Node(this.myAddr));
        this.watermarkBuffer = new WatermarkBuffer(K, H, L);
    }

    void startServer() throws IOException {
        server = ServerBuilder.forPort(myAddr.getPort())
                .addService(this)
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            this.stopServer();
            System.err.println("*** server shut down");
        }));
    }

    private void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Override
    public void receiveLinkUpdateMessage(final LinkUpdateMessageWire request,
                                         final StreamObserver<Response> responseObserver) {
        final LinkUpdateMessage msg = new LinkUpdateMessage(request.getSrc(), request.getDst(),
                                            request.getStatus());
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

        // If msg UP and in set, we'
        if (msg.getStatus().equals(Remoting.Status.UP) && membershipView.isPresent(new Node(msg.getDst()))) {
            throw new RuntimeException("Received UP message for node already in set");
        }
        if (msg.getStatus().equals(Remoting.Status.DOWN) && !membershipView.isPresent(new Node(msg.getDst()))) {
            throw new RuntimeException("Received DOWN message for node not in set");
        }

        final List<Node> proposal = proposedViewChange(msg);
        if (proposal.size() != 0) {
            // Initiate proposal
            throw new UnsupportedOperationException();
            // execute consensus.
            // if consensus
        }

        // continue gossipping
    }

    private List<Node> proposedViewChange(final LinkUpdateMessage msg) {
        return watermarkBuffer.receiveLinkUpdateMessage(msg);
    }

}