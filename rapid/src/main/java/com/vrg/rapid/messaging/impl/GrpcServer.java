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

package com.vrg.rapid.messaging.impl;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.MembershipService;
import com.vrg.rapid.SharedResources;
import com.vrg.rapid.messaging.IMessagingServer;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.MembershipServiceGrpc;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * gRPC server object. It defers receiving messages until it is ready to
 * host a MembershipService object.
 */
public class GrpcServer extends MembershipServiceGrpc.MembershipServiceImplBase implements IMessagingServer {
    private final ExecutorService grpcExecutor;
    @Nullable private final EventLoopGroup eventLoopGroup;
    private static final RapidResponse BOOTSTRAPPING_MESSAGE =
            RapidResponse.newBuilder().setProbeResponse(ProbeResponse.newBuilder()
                                                        .setStatus(NodeStatus.BOOTSTRAPPING).build()).build();
    private final Endpoint address;
    @Nullable
    private MembershipService membershipService;
    @Nullable private Server server;
    private final boolean useInProcessServer;

    // Used to queue messages in the RPC layer until we are ready with
    // a MembershipService object
    public GrpcServer(final Endpoint address, final SharedResources sharedResources,
                      final boolean useInProcessTransport) {
        this.address = address;
        this.grpcExecutor = sharedResources.getServerExecutor();
        this.eventLoopGroup = useInProcessTransport ? null : sharedResources.getEventLoopGroup();
        this.useInProcessServer = useInProcessTransport;
    }


    /**
     * Defined in rapid.proto.
     */
    @Override
    public void sendRequest(final RapidRequest rapidRequest,
                            final StreamObserver<RapidResponse> responseObserver) {
        if (membershipService != null) {
            final ListenableFuture<RapidResponse> result = membershipService.handleMessage(rapidRequest);
            Futures.addCallback(result, new ResponseCallback(responseObserver), grpcExecutor);
        }
        else if (rapidRequest.getContentCase().equals(RapidRequest.ContentCase.PROBEMESSAGE)) {
            /*
             * This is a special case which indicates that:
             *  1) the system is configured to use a failure detector that relies on Rapid's probe messages
             *  2) the node receiving the probe message has been added to the cluster but has not yet completed
             *     its bootstrap process (has not received its join-confirmation yet).
             *  3) By virtue of 2), the node is "about to be up" and therefore informs the monitor that it is
             *     still bootstrapping. This extra information may or may not be respected by the failure detector,
             *     but is useful in large deployments.
             */
            responseObserver.onNext(BOOTSTRAPPING_MESSAGE);
            responseObserver.onCompleted();
        }
    }

    /**
     * Invoked by the bootstrap protocol when it has a membership service object
     * ready. Until this method is called, the GrpcServer will not have its gRPC service
     * methods invoked.
     *
     * @param service a fully initialized MembershipService object.
     */
    @Override
    public void setMembershipService(final MembershipService service) {
        if (this.membershipService != null) {
            throw new RuntimeException("setMembershipService called more than once");
        }
        this.membershipService = service;
    }


    // IMessaging server interface
    @Override
    public void shutdown() {
        assert server != null;
        try {
            server.shutdown();
            server.awaitTermination(0, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts the RPC server.
     *
     * @throws IOException if a server cannot be successfully initialized
     */
    @Override
    public void start() throws IOException {
        if (useInProcessServer) {
            final ServerBuilder builder = InProcessServerBuilder.forName(address.toString());
            server = builder.addService(this)
                    .executor(grpcExecutor)
                    .compressorRegistry(CompressorRegistry.getDefaultInstance())
                    .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                    .build()
                    .start();
        } else {
            server = NettyServerBuilder.forAddress(new InetSocketAddress(address.getHostname(), address.getPort()))
                    .workerEventLoopGroup(eventLoopGroup)
                    .addService(this)
                    .executor(grpcExecutor)
                    .compressorRegistry(CompressorRegistry.getDefaultInstance())
                    .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                    .build()
                    .start();
        }

        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    // Callbacks
    private static class ResponseCallback implements FutureCallback<RapidResponse> {
        private final StreamObserver<RapidResponse> responseObserver;

        ResponseCallback(final StreamObserver<RapidResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onSuccess(@Nullable final RapidResponse response) {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void onFailure(final Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
