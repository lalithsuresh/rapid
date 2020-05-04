/*
 * Copyright © 2016 - 2020 VMware, Inc. All Rights Reserved.
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
import com.google.protobuf.ByteString;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.messaging.IMessagingServer;
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.messaging.impl.GrpcServer;
import com.vrg.rapid.monitoring.IEdgeFailureDetectorFactory;
import com.vrg.rapid.monitoring.impl.PingPongFailureDetector;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.RapidRequest;
import io.grpc.ExperimentalApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * The public API for Rapid. Users create Cluster objects using either Cluster.start()
 * or Cluster.join(), depending on whether the user is starting a new cluster or not:
 *
 * <pre>
 * {@code
 *   Endpoint seedAddress = Endpoint.hostFromString("127.0.0.1", 1234);
 *   Cluster c = Cluster.Builder(seedAddress).start();
 *   ...
 *   Endpoint joinerAddress = Endpoint.hostFromString("127.0.0.1", 1235);
 *   Cluster c = Cluster.Builder(joinerAddress).join(seedAddress);
 * }
 * </pre>
 *
 * The API does not yet support a node, as identified by a hostname and port, to
 * be part of multiple separate clusters.
 */
public final class Cluster {
    private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);
    private static final int K = 10;
    private static final int H = 9;
    private static final int L = 4;
    private static final int RETRIES = 5;
    private final MembershipService membershipService;
    private final IMessagingServer rpcServer;
    private final SharedResources sharedResources;
    private final Endpoint listenAddress;
    private boolean hasShutdown = false;

    private Cluster(final IMessagingServer rpcServer,
                    final MembershipService membershipService,
                    final SharedResources sharedResources,
                    final Endpoint listenAddress) {
        this.membershipService = membershipService;
        this.rpcServer = rpcServer;
        this.sharedResources = sharedResources;
        this.listenAddress = listenAddress;
    }

    /**
     * Returns the list of endpoints currently in the membership set.
     *
     * @return list of endpoints in the membership set
     * @throws IllegalStateException when trying to get the memberlist after shutting down
     */
    public List<Endpoint> getMemberlist() {
        if (hasShutdown) {
            throw new IllegalStateException("Can't access the memberlist after having shut down");
        }
        return membershipService.getMembershipView();
    }

    /**
     * Returns the number of endpoints currently in the membership set.
     *
     * @return the number of endpoints in the membership set
     * @throws IllegalStateException when trying to get the membership size after shutting down
     */
    public int getMembershipSize() {
        if (hasShutdown) {
            throw new IllegalStateException("Can't access the memberlist after having shut down");
        }
        return membershipService.getMembershipSize();
    }

    /**
     * Returns the list of endpoints currently in the membership set.
     *
     * @return list of endpoints in the membership set
     * @throws IllegalStateException when trying to get the cluster metadata after shutting down
     */
    public Map<Endpoint, Metadata> getClusterMetadata() {
        if (hasShutdown) {
            throw new IllegalStateException("Can't access the memberlist after having shut down");
        }
        return membershipService.getMetadata();
    }

    /**
     * Register callbacks for cluster events.
     *
     * @param event Cluster event to subscribe to
     * @param callback Callback to be executed when {@code event} occurs.
     */
    public void registerSubscription(final ClusterEvents event,
                                     final BiConsumer<Long, List<NodeStatusChange>> callback) {
        membershipService.registerSubscription(event, callback);
    }

    /**
     * Gracefully leaves the cluster by informing observers of the intent and then shuts down the entire system
     */
    public void leaveGracefully() {
        LOG.debug("Leaving the membership group and shutting down");
        membershipService.leave();
        shutdown();
    }

    /**
     * Shuts down the entire system
     */
    public void shutdown() {
        LOG.debug("Shutting down RpcServer and MembershipService");
        rpcServer.shutdown();
        membershipService.shutdown();
        sharedResources.shutdown();
        this.hasShutdown = true;
    }

    public static class Builder {
        private final Endpoint listenAddress;
        @Nullable private IEdgeFailureDetectorFactory edgeFailureDetector = null;
        private Metadata metadata = Metadata.getDefaultInstance();
        private Settings settings = new Settings();
        private final Map<ClusterEvents, List<BiConsumer<Long, List<NodeStatusChange>>>> subscriptions =
                new EnumMap<>(ClusterEvents.class);

        // These fields are initialized at the beginning of start() and join()
        @Nullable private IMessagingClient messagingClient = null;
        @Nullable private IMessagingServer messagingServer = null;
        @Nullable private SharedResources sharedResources = null;
        @Nullable private Endpoint seedAddress = null;

        /**
         * Instantiates a builder for a Rapid Cluster node that will listen on the given {@code listenAddress}
         *
         * @param listenAddress The listen address of the node being instantiated
         */
        public Builder(final HostAndPort listenAddress) {
            this.listenAddress = Endpoint.newBuilder()
                    .setHostname(ByteString.copyFromUtf8(listenAddress.getHost()))
                    .setPort(listenAddress.getPort())
                    .build();
        }

        /**
         * Instantiates a builder for a Rapid Cluster node that will listen on the given {@code listenAddress}
         *
         * @param listenAddress The listen address of the node being instantiated
         */
        Builder(final Endpoint listenAddress) {
            this.listenAddress = listenAddress;
        }

        /**
         * Set static application-specific metadata that is associated with the node being instantiated.
         * This may include tags like "role":"frontend".
         *
         * @param metadata A map specifying a set of key-value tags.
         */
        @ExperimentalApi
        public Builder setMetadata(final Map<String, ByteString> metadata) {
            Objects.requireNonNull(metadata);
            this.metadata = Metadata.newBuilder().putAllMetadata(metadata).build();
            return this;
        }

        /**
         * Set a link failure detector to use for observers to watch their subjects.
         *
         * @param edgeFailureDetector A link failure detector used as input for Rapid's failure detection.
         */
        @ExperimentalApi
        public Builder setEdgeFailureDetectorFactory(final IEdgeFailureDetectorFactory edgeFailureDetector) {
            Objects.requireNonNull(edgeFailureDetector);
            this.edgeFailureDetector = edgeFailureDetector;
            return this;
        }

        /**
         * This is used to register subscriptions for different events
         */
        @ExperimentalApi
        public Builder addSubscription(final ClusterEvents event,
                                       final BiConsumer<Long, List<NodeStatusChange>> callback) {
            this.subscriptions.computeIfAbsent(event, k -> new ArrayList<>());
            this.subscriptions.get(event).add(callback);
            return this;
        }

        /**
         * This is used to configure Cluster properties such as timeouts
         */
        public Builder useSettings(final Settings settings) {
            this.settings = settings;
            return this;
        }

        /**
         * Supply the messaging client and server to use.
         */
        public Builder setMessagingClientAndServer(final IMessagingClient messagingClient,
                                                   final IMessagingServer messagingServer) {
            this.messagingClient = messagingClient;
            this.messagingServer = messagingServer;
            return this;
        }

        /**
         * Start a cluster without joining. Required to bootstrap a seed node.
         *
         * @throws IOException Thrown if we cannot successfully start a server
         */
        public Cluster start() throws IOException {
            Objects.requireNonNull(listenAddress);
            sharedResources = new SharedResources(listenAddress);
            messagingServer = messagingServer != null
                            ? messagingServer
                            : new GrpcServer(listenAddress, sharedResources, settings.getUseInProcessTransport());
            messagingClient = messagingClient != null
                                ? messagingClient
                                : new GrpcClient(listenAddress, sharedResources, settings);
            final NodeId currentIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
            final MembershipView membershipView = new MembershipView(K, Collections.singletonList(currentIdentifier),
                    Collections.singletonList(listenAddress));
            final MultiNodeCutDetector cutDetector = new MultiNodeCutDetector(K, H, L);
            edgeFailureDetector = edgeFailureDetector != null ? edgeFailureDetector
                    : new PingPongFailureDetector.Factory(listenAddress, messagingClient);

            final Map<Endpoint, Metadata> metadataMap = metadata.getMetadataCount() > 0
                                                    ? Collections.singletonMap(listenAddress, metadata)
                                                    : Collections.emptyMap();
            final MembershipService membershipService = new MembershipService(listenAddress,
                    cutDetector, membershipView, sharedResources, settings,
                                            messagingClient, edgeFailureDetector, metadataMap, subscriptions);
            messagingServer.setMembershipService(membershipService);
            messagingServer.start();
            return new Cluster(messagingServer, membershipService, sharedResources, listenAddress);
        }


        /**
         * Joins an existing cluster, using {@code seedAddress} to bootstrap.
         *
         * @param seedHostAndPort Seed node for the bootstrap protocol
         * @throws IOException Thrown if we cannot successfully start a server
         */
        public Cluster join(final HostAndPort seedHostAndPort) throws IOException, InterruptedException {
            final Endpoint seedAddress = Endpoint.newBuilder()
                    .setHostname(ByteString.copyFromUtf8(seedHostAndPort.getHost()))
                    .setPort(seedHostAndPort.getPort())
                    .build();
            return join(seedAddress);
        }

        /**
         * Joins an existing cluster, using {@code seedAddress} to bootstrap.
         *
         * @param seedAddress Seed node for the bootstrap protocol
         * @throws IOException Thrown if we cannot successfully start a server
         */
        Cluster join(final Endpoint seedAddress) throws IOException, InterruptedException {
            if (this.seedAddress != null && !this.seedAddress.equals(seedAddress)) {
                throw new JoinException("Cannot join another seed node while an attempt is in progress");
            }
            this.seedAddress = seedAddress;

            NodeId currentIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
            sharedResources = new SharedResources(listenAddress);
            messagingServer = messagingServer != null
                    ? messagingServer
                    : new GrpcServer(listenAddress, sharedResources, settings.getUseInProcessTransport());
            messagingClient = messagingClient != null
                    ? messagingClient
                    : new GrpcClient(listenAddress, sharedResources, settings);
            messagingServer.start();

            for (int attempt = 0; attempt < RETRIES; attempt++) {
                try {
                    return joinAttempt(seedAddress, currentIdentifier, attempt);
                } catch (final ExecutionException e) {
                    LOG.error("Join message to seed {} returned an exception: {}", Utils.loggable(seedAddress), e);
                } catch (final JoinPhaseOneException e) {
                    /*
                     * These are error responses from a seed node that warrant a retry.
                     */
                    final JoinResponse result = e.getJoinPhaseOneResult();
                    switch (result.getStatusCode()) {
                        case UUID_ALREADY_IN_RING:
                            LOG.error("UUID_ALREADY_IN_RING received from {}. Retrying.",
                                    Utils.loggable(result.getSender()));
                            currentIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
                            break;
                        case HOSTNAME_ALREADY_IN_RING:
                            LOG.error("Membership rejected by {}. Retrying.", Utils.loggable(result.getSender()));
                            break;
                        default:
                            this.seedAddress = null;
                            throw new JoinException("Unrecognized status code");
                    }
                }
            }
            messagingServer.shutdown();
            messagingClient.shutdown();
            sharedResources.shutdown();
            this.seedAddress = null;
            throw new JoinException("Join attempt unsuccessful " + Utils.loggable(listenAddress));
        }

        /**
         * A single attempt by a node to join a cluster.
         */
        private Cluster joinAttempt(final Endpoint seedAddress, final NodeId currentIdentifier, final int attempt)
                                                                throws ExecutionException, InterruptedException {
            assert messagingClient != null;

            final RapidRequest joinMessage = Utils.toRapidRequest(JoinMessage.newBuilder()
                    .setSender(listenAddress)
                    .setNodeId(currentIdentifier)
                    .setMetadata(metadata)
                    .build());
            final JoinResponse joinResponse = messagingClient.sendMessage(seedAddress, joinMessage)
                    .get()
                    .getJoinResponse();

            if (joinResponse.getStatusCode() != JoinStatusCode.SAFE_TO_JOIN) {
                throw new JoinPhaseOneException(joinResponse);
            }

            return createClusterFromJoinResponse(joinResponse);
        }

        /**
         * We have a valid JoinResponse. Use the retrieved configuration to construct and return a Cluster object.
         */
        private Cluster createClusterFromJoinResponse(final JoinResponse response) {
            assert messagingClient != null && messagingServer != null && sharedResources != null;
            // Safe to proceed. Extract the list of endpoints and identifiers from the message,
            // assemble a MembershipService object and start an RpcServer.
            final List<Endpoint> allEndpoints = response.getEndpointsList();
            final List<NodeId> identifiersSeen = response.getIdentifiersList();
            final Map<Endpoint, Metadata> allMetadata = new HashMap<>();
            for (int i = 0; i < response.getMetadataKeysCount(); i++) {
                final Endpoint key = response.getMetadataKeys(i);
                final Metadata value = response.getMetadataValues(i);
                allMetadata.put(key, value);
            }

            assert !identifiersSeen.isEmpty();
            assert !allEndpoints.isEmpty();

            final MembershipView membershipViewFinal =
                    new MembershipView(K, identifiersSeen, allEndpoints);
            final MultiNodeCutDetector cutDetector = new MultiNodeCutDetector(K, H, L);
            edgeFailureDetector = edgeFailureDetector != null ? edgeFailureDetector
                                                  : new PingPongFailureDetector.Factory(listenAddress, messagingClient);
            final MembershipService membershipService =
                    new MembershipService(listenAddress, cutDetector, membershipViewFinal,
                           sharedResources, settings, messagingClient, edgeFailureDetector, allMetadata, subscriptions);
            messagingServer.setMembershipService(membershipService);
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} has observers {}", listenAddress,
                        membershipViewFinal.getObserversOf(listenAddress));
                LOG.trace("{} has subjects {}", listenAddress,
                        membershipViewFinal.getObserversOf(listenAddress));
            }
            return new Cluster(messagingServer, membershipService, sharedResources, listenAddress);
        }
    }


    @Override
    public String toString() {
        return "Cluster:" + listenAddress.getHostname().toStringUtf8() + ":" + listenAddress.getPort();
    }

    public static final class JoinException extends RuntimeException {
        JoinException(final String msg) {
            super(msg);
        }
    }

    static final class JoinPhaseOneException extends RuntimeException {
        final JoinResponse joinPhaseOneResult;

        JoinPhaseOneException(final JoinResponse joinResult) {
            this.joinPhaseOneResult = joinResult;
        }

        private JoinResponse getJoinPhaseOneResult() {
            return joinPhaseOneResult;
        }
    }

}
