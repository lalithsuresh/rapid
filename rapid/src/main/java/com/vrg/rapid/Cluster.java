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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.vrg.rapid.messaging.GrpcClient;
import com.vrg.rapid.messaging.GrpcServer;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.messaging.IMessagingServer;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;
import com.vrg.rapid.monitoring.PingPongFailureDetector;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.NodeId;
import io.grpc.ClientInterceptor;
import io.grpc.ExperimentalApi;
import io.grpc.Internal;
import io.grpc.ServerInterceptor;
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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The public API for Rapid. Users create Cluster objects using either Cluster.start()
 * or Cluster.join(), depending on whether the user is starting a new cluster or not:
 *
 * <pre>
 * {@code
 *   HostAndPort seedAddress = HostAndPort.fromString("127.0.0.1", 1234);
 *   Cluster c = Cluster.Builder(seedAddress).start();
 *   ...
 *   HostAndPort joinerAddress = HostAndPort.fromString("127.0.0.1", 1235);
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
    private final HostAndPort listenAddress;

    private Cluster(final IMessagingServer rpcServer,
                    final MembershipService membershipService,
                    final SharedResources sharedResources,
                    final HostAndPort listenAddress) {
        this.membershipService = membershipService;
        this.rpcServer = rpcServer;
        this.sharedResources = sharedResources;
        this.listenAddress = listenAddress;
    }

    public static class Builder {
        private final HostAndPort listenAddress;
        @Nullable private ILinkFailureDetectorFactory linkFailureDetector = null;
        private Metadata metadata = Metadata.getDefaultInstance();
        private List<ServerInterceptor> serverInterceptors = Collections.emptyList();
        private List<ClientInterceptor> clientInterceptors = Collections.emptyList();
        private Settings settings = new Settings();
        private final Map<ClusterEvents, List<Consumer<List<NodeStatusChange>>>> subscriptions =
                new EnumMap<>(ClusterEvents.class);

        // These fields are initialized at the beginning of start() and join()
        @Nullable private IMessagingClient messagingClient = null;
        @Nullable private IMessagingServer rpcServer = null;
        @Nullable private SharedResources sharedResources = null;

        /**
         * Instantiates a builder for a Rapid Cluster node that will listen on the given {@code listenAddress}
         *
         * @param listenAddress The listen address of the node being instantiated
         */
        public Builder(final HostAndPort listenAddress) {
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
         * Set a link failure detector to use for monitors to watch their monitorees.
         *
         * @param linkFailureDetector A link failure detector used as input for Rapid's failure detection.
         */
        @ExperimentalApi
        public Builder setLinkFailureDetectorFactory(final ILinkFailureDetectorFactory linkFailureDetector) {
            Objects.requireNonNull(linkFailureDetector);
            this.linkFailureDetector = linkFailureDetector;
            return this;
        }

        /**
         * This is used to register subscriptions for different events
         */
        @ExperimentalApi
        public Builder addSubscription(final ClusterEvents event,
                                       final Consumer<List<NodeStatusChange>> callback) {
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
         * This is used by tests to inject message drop interceptors at the RpcServer.
         */
        @Internal
        Builder setServerInterceptors(final List<ServerInterceptor> interceptors) {
            this.serverInterceptors = interceptors;
            return this;
        }

        /**
         * This is used by tests to inject message drop interceptors at the client channels.
         */
        @Internal
        Builder setClientInterceptors(final List<ClientInterceptor> interceptors) {
            this.clientInterceptors = interceptors;
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
            rpcServer = new GrpcServer(listenAddress, sharedResources, serverInterceptors,
                                       settings.getUseInProcessTransport());
            messagingClient = new GrpcClient(listenAddress, Collections.emptyList(), sharedResources, settings);
            final NodeId currentIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
            final MembershipView membershipView = new MembershipView(K, Collections.singletonList(currentIdentifier),
                    Collections.singletonList(listenAddress));
            final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
            linkFailureDetector = linkFailureDetector != null ? linkFailureDetector
                    : new PingPongFailureDetector.Factory(listenAddress, messagingClient);
            final MembershipService.Builder builder = new MembershipService.Builder(listenAddress, watermarkBuffer,
                    membershipView, sharedResources, settings)
                    .setMessagingClient(messagingClient)
                    .setSubscriptions(subscriptions)
                    .setLinkFailureDetector(linkFailureDetector);
            final MembershipService membershipService = metadata.getMetadataCount() > 0
                    ? builder.setMetadata(Collections.singletonMap(listenAddress.toString(), metadata)).build()
                    : builder.build();
            rpcServer.setMembershipService(membershipService);
            rpcServer.start();
            return new Cluster(rpcServer, membershipService, sharedResources, listenAddress);
        }


        /**
         * Joins an existing cluster, using {@code seedAddress} to bootstrap.
         *
         * @param seedAddress Seed node for the bootstrap protocol
         * @throws IOException Thrown if we cannot successfully start a server
         */
        public Cluster join(final HostAndPort seedAddress) throws IOException, InterruptedException {
            NodeId currentIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
            sharedResources = new SharedResources(listenAddress);
            rpcServer = new GrpcServer(listenAddress, sharedResources, serverInterceptors,
                                       settings.getUseInProcessTransport());
            messagingClient = new GrpcClient(listenAddress, clientInterceptors, sharedResources, settings);
            rpcServer.start();
            for (int attempt = 0; attempt < RETRIES; attempt++) {
                try {
                    return joinAttempt(seedAddress, currentIdentifier, attempt);
                } catch (final ExecutionException | JoinPhaseTwoException e) {
                    LOG.error("Join message to seed {} returned an exception: {}", seedAddress, e.toString());
                } catch (final JoinPhaseOneException e) {
                    /*
                     * These are error responses from a seed node that warrant a retry.
                     */
                    final JoinResponse result = e.getJoinPhaseOneResult();
                    switch (result.getStatusCode()) {
                        case CONFIG_CHANGED:
                            LOG.error("CONFIG_CHANGED received from {}. Retrying.", result.getSender());
                            break;
                        case UUID_ALREADY_IN_RING:
                            LOG.error("UUID_ALREADY_IN_RING received from {}. Retrying.", result.getSender());
                            currentIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
                            break;
                        case MEMBERSHIP_REJECTED:
                            LOG.error("Membership rejected by {}. Retrying.", result.getSender());
                            break;
                        default:
                            throw new JoinException("Unrecognized status code");
                    }
                }
            }
            rpcServer.shutdown();
            messagingClient.shutdown();
            sharedResources.shutdown();
            throw new JoinException("Join attempt unsuccessful " + listenAddress);
        }

        /**
         * A single attempt by a node to join a cluster. This includes phase one, where it contacts
         * a seed node to receive a list of monitors to contact and the configuration to join. If successful,
         * it triggers phase two where it contacts those monitors who then vouch for the joiner's admission
         * into the cluster.
         */
        private Cluster joinAttempt(final HostAndPort seedAddress, final NodeId currentIdentifier, final int attempt)
                                                                throws ExecutionException, InterruptedException {
            assert messagingClient != null;
            // First, get the configuration ID and the monitors to contact from the seed node.
            final JoinResponse joinPhaseOneResult = messagingClient.sendJoinMessage(seedAddress,
                    listenAddress, currentIdentifier).get();

            /*
             * Either the seed node indicates it is safe to join, or it indicates that we're already
             * part of the configuration (which happens due to a race condition where we retry a join
             * after a timeout while the cluster has added us -- see below).
             */
            if (joinPhaseOneResult.getStatusCode() != JoinStatusCode.SAFE_TO_JOIN
                    && joinPhaseOneResult.getStatusCode() != JoinStatusCode.HOSTNAME_ALREADY_IN_RING) {
                throw new JoinPhaseOneException(joinPhaseOneResult);
            }

            /*
             * HOSTNAME_ALREADY_IN_RING is a special case. If the joinPhase2 request times out before
             * the join confirmation arrives from a monitor, a client may re-try a join by contacting
             * the seed and get this response. It should simply get the configuration streamed to it.
             * To do that, that client tries the join protocol but with a configuration id of -1.
             */
            final long configurationToJoin = joinPhaseOneResult.getStatusCode()
                    == JoinStatusCode.HOSTNAME_ALREADY_IN_RING ? -1 : joinPhaseOneResult.getConfigurationId();
            LOG.debug("{} is trying a join under configuration {} (attempt {})",
                    listenAddress, configurationToJoin, attempt);

            /*
             * Phase one complete. Now send a phase two message to all our monitors, and if there is a valid
             * response, construct a Cluster object based on it.
             */
            final Optional<JoinResponse> response = sendJoinPhase2Messages(joinPhaseOneResult,
                    configurationToJoin, currentIdentifier)
                    .stream()
                    .filter(Objects::nonNull)
                    .filter(r -> r.getStatusCode() == JoinStatusCode.SAFE_TO_JOIN)
                    .filter(r -> r.getConfigurationId() != configurationToJoin)
                    .findFirst();
            if (response.isPresent()) {
                return createClusterFromJoinResponse(response.get());
            }
            throw new JoinPhaseTwoException();
        }

        /**
         * Identifies the set of monitors to reach out to from the phase one message, and sends a join phase 2 message.
         */
        private List<JoinResponse> sendJoinPhase2Messages(final JoinResponse joinPhaseOneResult,
                                        final long configurationToJoin, final NodeId currentIdentifier)
                                                            throws ExecutionException, InterruptedException {
            assert messagingClient != null;
            // We have the list of monitors. Now contact them as part of phase 2.
            final List<HostAndPort> monitorList = joinPhaseOneResult.getHostsList().stream()
                    .map(HostAndPort::fromString)
                    .collect(Collectors.toList());
            final Map<HostAndPort, List<Integer>> ringNumbersPerMonitor = new HashMap<>(K);

            // Batch together requests to the same node.
            int ringNumber = 0;
            for (final HostAndPort monitor: monitorList) {
                ringNumbersPerMonitor.computeIfAbsent(monitor, k -> new ArrayList<>()).add(ringNumber);
                ringNumber++;
            }

            final List<ListenableFuture<JoinResponse>> responseFutures = new ArrayList<>();
            for (final Map.Entry<HostAndPort, List<Integer>> entry: ringNumbersPerMonitor.entrySet()) {
                final JoinMessage msg = JoinMessage.newBuilder()
                        .setSender(listenAddress.toString())
                        .setNodeId(currentIdentifier)
                        .setMetadata(metadata)
                        .setConfigurationId(configurationToJoin)
                        .addAllRingNumber(entry.getValue()).build();
                LOG.info("{} is sending a join-p2 to {} for config {}",
                        listenAddress, entry.getKey(), configurationToJoin);
                final ListenableFuture<JoinResponse> call = messagingClient.sendJoinPhase2Message(entry.getKey(), msg);
                responseFutures.add(call);
            }
            return Futures.successfulAsList(responseFutures).get();
        }

        /**
         * We have a valid JoinPhase2Response. Use the retrieved configuration to construct and return a Cluster object.
         */
        private Cluster createClusterFromJoinResponse(final JoinResponse response) {
            assert messagingClient != null && rpcServer != null && sharedResources != null;
            // Safe to proceed. Extract the list of hosts and identifiers from the message,
            // assemble a MembershipService object and start an RpcServer.
            final List<HostAndPort> allHosts = response.getHostsList().stream()
                    .map(HostAndPort::fromString)
                    .collect(Collectors.toList());
            final List<NodeId> identifiersSeen = response.getIdentifiersList();

            final Map<String, Metadata> allMetadata = response.getClusterMetadataMap();

            assert !identifiersSeen.isEmpty();
            assert !allHosts.isEmpty();

            final MembershipView membershipViewFinal =
                    new MembershipView(K, identifiersSeen, allHosts);
            final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
            linkFailureDetector = linkFailureDetector != null ? linkFailureDetector
                                                  : new PingPongFailureDetector.Factory(listenAddress, messagingClient);
            final MembershipService membershipService =
                    new MembershipService.Builder(listenAddress, watermarkBuffer, membershipViewFinal,
                                                  sharedResources, settings)
                                         .setMessagingClient(messagingClient)
                                         .setLinkFailureDetector(linkFailureDetector)
                                         .setMetadata(allMetadata)
                                         .setSubscriptions(subscriptions)
                                         .build();
            rpcServer.setMembershipService(membershipService);
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} has monitors {}", listenAddress,
                        membershipViewFinal.getMonitorsOf(listenAddress));
                LOG.trace("{} has monitorees {}", listenAddress,
                        membershipViewFinal.getMonitorsOf(listenAddress));
            }
            return new Cluster(rpcServer, membershipService, sharedResources, listenAddress);
        }
    }

    /**
     * Returns the list of hosts currently in the membership set.
     *
     * @return list of hosts in the membership set
     */
    public List<HostAndPort> getMemberlist() {
        return membershipService.getMembershipView();
    }

    /**
     * Returns the number of hosts currently in the membership set.
     *
     * @return the number of hosts in the membership set
     */
    public int getMembershipSize() {
        return membershipService.getMembershipSize();
    }

    /**
     * Returns the list of hosts currently in the membership set.
     *
     * @return list of hosts in the membership set
     */
    public Map<String, Metadata> getClusterMetadata() {
        return membershipService.getMetadata();
    }

    /**
     * Register callbacks for cluster events.
     *
     * @param event Cluster event to subscribe to
     * @param callback Callback to be executed when {@code event} occurs.
     */
    public void registerSubscription(final ClusterEvents event,
                                     final Consumer<List<NodeStatusChange>> callback) {
        membershipService.registerSubscription(event, callback);
    }

    /**
     * Shutdown the RpcServer
     */
    public void shutdown() {
        LOG.debug("Shutting down RpcServer and MembershipService");
        rpcServer.shutdown();
        membershipService.shutdown();
        sharedResources.shutdown();
    }

    @Override
    public String toString() {
        return "Cluster:" + listenAddress;
    }

    public static final class JoinException extends RuntimeException {
        JoinException(final String msg) {
            super(msg);
        }
    }

    static final class JoinPhaseOneException extends RuntimeException {
        final JoinResponse joinPhaseOneResult;

        JoinPhaseOneException(final JoinResponse joinPhaseOneResult) {
            this.joinPhaseOneResult = joinPhaseOneResult;
        }

        private JoinResponse getJoinPhaseOneResult() {
            return joinPhaseOneResult;
        }
    }

    static final class JoinPhaseTwoException extends RuntimeException {
    }
}
