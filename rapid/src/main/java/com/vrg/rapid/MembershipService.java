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
import com.vrg.rapid.monitoring.ILinkFailureDetector;
import com.vrg.rapid.monitoring.PingPongFailureDetector;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.Response;
import io.grpc.ExperimentalApi;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Membership server class that implements the Rapid protocol.
 *
 * Note: This class is not thread-safe yet. RpcServer.start() uses a single threaded messagingExecutor during the server
 * initialization to make sure that only a single thread runs the process* methods.
 *
 */
@NotThreadSafe
final class MembershipService {
    private static final Logger LOG = LoggerFactory.getLogger(MembershipService.class);
    private static final int BATCHING_WINDOW_IN_MS = 100;
    static int FAILURE_DETECTOR_INTERVAL_IN_MS = 1000;
    static int FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 2000;
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final HostAndPort myAddr;
    private final boolean logProposals;
    private final IBroadcaster broadcaster;
    private final Set<HostAndPort> joinersToRespondTo = new HashSet<>();
    private final Map<HostAndPort, UUID> joinerUuid = new HashMap<>(); // XXX: Not being cleared.
    private final List<Set<HostAndPort>> logProposalList = new ArrayList<>();
    private final LinkFailureDetectorRunner linkFailureDetectorRunner;
    private final RpcClient rpcClient;
    private final MetadataManager metadataManager;
    private final boolean isExternalConsensusEnabled;

    // Event subscriptions
    private final Map<ClusterEvents, List<Consumer<List<NodeStatusChange>>>> subscriptions;

    // Fields used by batching logic.
    @GuardedBy("batchSchedulerLock")
    private long lastEnqueueTimestamp = -1;    // Timestamp
    @GuardedBy("batchSchedulerLock")
    private final LinkedBlockingQueue<LinkUpdateMessage> sendQueue = new LinkedBlockingQueue<>();
    private final Lock batchSchedulerLock = new ReentrantLock();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    // Fields used by consensus protocol
    private final Map<List<String>, AtomicInteger> votesPerProposal = new HashMap<>();
    private boolean announcedProposal = false;

    static class Builder {
        private final MembershipView membershipView;
        private final WatermarkBuffer watermarkBuffer;
        private final HostAndPort myAddr;
        private boolean logProposals;
        private boolean isExternalConsensusEnabled = false;
        private Map<String, String> metadata = Collections.emptyMap();
        private ILinkFailureDetector linkFailureDetector;

        Builder(final HostAndPort myAddr,
                final WatermarkBuffer watermarkBuffer,
                final MembershipView membershipView) {
            this.myAddr = Objects.requireNonNull(myAddr);
            this.watermarkBuffer = Objects.requireNonNull(watermarkBuffer);
            this.membershipView = Objects.requireNonNull(membershipView);
            this.linkFailureDetector = new PingPongFailureDetector(myAddr);
        }

        Builder setLogProposals(final boolean logProposals) {
            this.logProposals = logProposals;
            return this;
        }

        Builder setMetadata(final Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        Builder setLinkFailureDetector(final ILinkFailureDetector linkFailureDetector) {
            this.linkFailureDetector = linkFailureDetector;
            return this;
        }

        MembershipService build() {
            return new MembershipService(this);
        }
    }

    private MembershipService(final Builder builder) {
        this.myAddr = builder.myAddr;
        this.membershipView = builder.membershipView;
        this.watermarkBuffer = builder.watermarkBuffer;
        this.logProposals = builder.logProposals;
        this.metadataManager = new MetadataManager();
        this.metadataManager.setMetadata(myAddr, builder.metadata);
        this.rpcClient = new RpcClient(myAddr);
        this.broadcaster = new UnicastToAllBroadcaster(rpcClient, scheduledExecutorService);
        this.isExternalConsensusEnabled = builder.isExternalConsensusEnabled;

        this.subscriptions = new HashMap<>(ClusterEvents.values().length); // One for each event.
        this.subscriptions.put(ClusterEvents.VIEW_CHANGE, new ArrayList<>(1));
        this.subscriptions.put(ClusterEvents.VIEW_CHANGE_PROPOSAL, new ArrayList<>(1));

        // Schedule background jobs
        this.scheduledExecutorService.scheduleAtFixedRate(new LinkUpdateBatcher(),
                0, BATCHING_WINDOW_IN_MS, TimeUnit.MILLISECONDS);

        // this::linkFailureNotification is invoked by the failure detector whenever an edge
        // to a monitor is marked faulty.
        this.linkFailureDetectorRunner = new LinkFailureDetectorRunner(builder.linkFailureDetector, rpcClient);
        this.linkFailureDetectorRunner.registerSubscription(this::linkFailureNotification);

        // This primes the link failure detector with the initial set of monitorees.
        linkFailureDetectorRunner.updateMembership(membershipView.getMonitoreesOf(myAddr));
        this.scheduledExecutorService.scheduleAtFixedRate(linkFailureDetectorRunner,
                FAILURE_DETECTOR_INITIAL_DELAY_IN_MS, FAILURE_DETECTOR_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * This is invoked by a new node joining the network at a seed node.
     * The seed responds with the current configuration ID and a list of monitors
     * for the joiner, who then moves on to phase 2 of the protocol with its monitors.
     */
    void processJoinMessage(final JoinMessage joinMessage,
                            final StreamObserver<JoinResponse> responseObserver) {
        final HostAndPort joiningHost = HostAndPort.fromString(joinMessage.getSender());
        final UUID uuid = UUID.fromString(joinMessage.getUuid());
        final JoinStatusCode statusCode = membershipView.isSafeToJoin(joiningHost, uuid);
        final JoinResponse.Builder builder = JoinResponse.newBuilder()
                .setSender(this.myAddr.toString())
                .setConfigurationId(membershipView.getCurrentConfigurationId())
                .setStatusCode(statusCode);
        LOG.trace("Join at seed for {seed:{}, sender:{}, config:{}, size:{}}",
                myAddr, joinMessage.getSender(),
                membershipView.getCurrentConfigurationId(), membershipView.getRing(0).size());
        if (statusCode.equals(JoinStatusCode.SAFE_TO_JOIN)) {
            // Return a list of monitors for the joiner to contact for phase 2 of the protocol
            builder.addAllHosts(membershipView.getExpectedMonitorsOf(joiningHost)
                   .stream()
                   .map(HostAndPort::toString)
                   .collect(Collectors.toList()));
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * Invoked by gatekeepers of a joining node. They perform any failure checking
     * required before propagating a LinkUpdateMessage with the status UP. After the watermarking
     * and consensus succeeds, the monitor informs the joiner about the new configuration it
     * is now a part of.
     */
    void processJoinPhaseTwoMessage(final JoinMessage joinMessage,
                                    final StreamObserver<Response> responseObserver) {
        final long currentConfiguration = membershipView.getCurrentConfigurationId();

        if (currentConfiguration == joinMessage.getConfigurationId()) {
            LOG.trace("Enqueuing SAFE_TO_JOIN for {sender:{}, monitor:{}, config:{}, size:{}}",
                    joinMessage.getSender(), myAddr,
                    currentConfiguration, membershipView.getRing(0).size());

            final LinkUpdateMessage msg = LinkUpdateMessage.newBuilder()
                    .setLinkSrc(this.myAddr.toString())
                    .setLinkDst(joinMessage.getSender())
                    .setLinkStatus(LinkStatus.UP)
                    .setConfigurationId(currentConfiguration)
                    .setUuid(joinMessage.getUuid())
                    .setRingNumber(joinMessage.getRingNumber())
                    .putAllMetadata(joinMessage.getMetadataMap())
                    .build();

            joinersToRespondTo.add(HostAndPort.fromString(joinMessage.getSender()));
            enqueueLinkUpdateMessage(msg);
            responseObserver.onNext(Response.getDefaultInstance()); // new config
            responseObserver.onCompleted();
        } else {
            // This handles the corner case where the configuration changed between phase 1 and phase 2
            // of the joining node's bootstrap. It should attempt to rejoin the network.
            final MembershipView.Configuration configuration = membershipView.getConfiguration();
            LOG.info("Wrong configuration for {sender:{}, monitor:{}, config:{}, size:{}}",
                    joinMessage.getSender(), myAddr,
                    currentConfiguration, membershipView.getRing(0).size());
            JoinResponse.Builder responseBuilder = JoinResponse.newBuilder()
                    .setSender(this.myAddr.toString())
                    .setConfigurationId(configuration.getConfigurationId());

            if (membershipView.isHostPresent(HostAndPort.fromString(joinMessage.getSender()))
                    && membershipView.isIdentifierPresent(UUID.fromString(joinMessage.getUuid()))) {

                // Race condition where a monitor already crossed H messages for the joiner and changed
                // the configuration, but the JoinPhase2 messages show up at the monitor
                // after it has already added the joiner. In this case, we simply
                // tell the sender that they're safe to join.
                responseBuilder = responseBuilder.setStatusCode(JoinStatusCode.SAFE_TO_JOIN)
                        .addAllHosts(configuration.hostAndPorts
                                .stream()
                                .map(HostAndPort::toString)
                                .collect(Collectors.toList()))
                        .addAllIdentifiers(configuration.uuids
                                .stream()
                                .map(UUID::toString)
                                .collect(Collectors.toList()));
            } else {
                responseBuilder = responseBuilder.setStatusCode(JoinStatusCode.CONFIG_CHANGED);
                LOG.info("Returning CONFIG_CHANGED for {sender:{}, monitor:{}, config:{}, size:{}}",
                        joinMessage.getSender(), myAddr,
                        configuration.getConfigurationId(), configuration.hostAndPorts.size());
            }
            final JoinResponse response = responseBuilder.build();
            scheduledExecutorService.execute(() -> {
                rpcClient.sendJoinConfirmation(HostAndPort.fromString(joinMessage.getSender()), response);
            });
            responseObserver.onNext(Response.getDefaultInstance()); // new config
            responseObserver.onCompleted();
        }
    }


    /**
     * This method receives link update events and delivers them to
     * the watermark buffer to check if it will return a valid
     * proposal.
     *
     * Link update messages that do not affect an ongoing proposal
     * needs to be dropped.
     */
    void processLinkUpdateMessage(final BatchedLinkUpdateMessage messageBatch) {
        Objects.requireNonNull(messageBatch);

        // We already have a proposal for this round => we have initiated consensus and cannot go back on our proposal.
        if (announcedProposal) {
            return;
        }

        // We proceed in three parts. First, we collect the messages from the batch that
        // are valid and do not violate conditions of the ring. These are packed in to
        // validMessages and processed later.
        final List<LinkUpdateMessage> validMessages = new ArrayList<>(messageBatch.getMessagesList().size());
        final long currentConfigurationId = membershipView.getCurrentConfigurationId();
        final int membershipSize = membershipView.getRing(0).size();

        for (final LinkUpdateMessage request: messageBatch.getMessagesList()) {
            final HostAndPort destination = HostAndPort.fromString(request.getLinkDst());
            LOG.trace("LinkUpdateMessage received {sender:{}, receiver:{}, config:{}, size:{}, status:{}}",
                    messageBatch.getSender(), myAddr,
                    request.getConfigurationId(),
                    membershipSize,
                    request.getLinkStatus());

            if (currentConfigurationId != request.getConfigurationId()) {
                LOG.trace("LinkUpdateMessage for configuration {} received during configuration {}",
                        request.getConfigurationId(), currentConfigurationId);
                continue;
            }

            // The invariant we want to maintain is that a node can only go into the
            // membership set once and leave it once.
            if (request.getLinkStatus().equals(LinkStatus.UP)
                    && membershipView.isHostPresent(destination)) {
                LOG.trace("LinkUpdateMessage with status UP received for node {} already in configuration {} ",
                        request.getLinkDst(), currentConfigurationId);
                continue;
            }
            if (request.getLinkStatus().equals(LinkStatus.DOWN)
                    && !membershipView.isHostPresent(destination)) {
                LOG.trace("LinkUpdateMessage with status DOWN received for node {} already in configuration {} ",
                        request.getLinkDst(), currentConfigurationId);
                continue;
            }

            if (request.getLinkStatus() == LinkStatus.UP) {
                joinerUuid.put(destination, UUID.fromString(request.getUuid()));

                // TODO: Ideally, we'd set the role only after the node has successfully joined.
                metadataManager.setMetadata(destination, request.getMetadataMap());
            }
            validMessages.add(request);
        }

        if (validMessages.size() == 0) {
            LOG.trace("All BatchLinkUpdateMessages received at node {} where invalid!", myAddr);
            return;
        }

        // We now batch apply all the valid messages into the watermark buffer, obtaining
        // a single aggregate membership change proposal to apply against the view.
        final Set<HostAndPort> proposal = new HashSet<>();
        for (final LinkUpdateMessage msg : validMessages) {
            proposal.addAll(watermarkBuffer.aggregateForProposal(msg));
        }

        proposal.addAll(watermarkBuffer.invalidateFailingLinks(membershipView));

        // If we have a proposal for this stage, start an instance of consensus on it.
        if (proposal.size() != 0) {
            if (logProposals) {
                logProposalList.add(proposal);
            }

            LOG.debug("Node {} has a proposal of size {}: {}", myAddr, proposal.size(), proposal);
            announcedProposal = true;

            if (subscriptions.containsKey(ClusterEvents.VIEW_CHANGE_PROPOSAL)) {
                final List<NodeStatusChange> result = createNodeStatusChangeList(proposal);
                // Inform subscribers that a proposal has been announced.
                subscriptions.get(ClusterEvents.VIEW_CHANGE_PROPOSAL).forEach(cb -> cb.accept(result));
            }
            if (!isExternalConsensusEnabled) {
                // TODO: Plug different consensus implementations here
                final ConsensusProposal proposalMessage = ConsensusProposal.newBuilder()
                        .setConfigurationId(currentConfigurationId)
                        .addAllHosts(proposal
                                .stream()
                                .map(HostAndPort::toString)
                                .sorted()
                                .collect(Collectors.toList()))
                        .setSender(myAddr.toString())
                        .build();
                broadcaster.broadcast(membershipView.getRing(0), proposalMessage);
            }
        }
    }


    /**
     * Receives proposal for the one-step consensus (essentially phase 2 of Fast Paxos).
     *
     * XXX: Implement recovery for the extremely rare possibility of conflicting proposals.
     *
     */
    void processConsensusProposal(final ConsensusProposal proposalMessage) {
        assert !isExternalConsensusEnabled;
        final long currentConfigurationId = membershipView.getCurrentConfigurationId();
        final long membershipSize = membershipView.getRing(0).size();

        if (proposalMessage.getConfigurationId() != currentConfigurationId) {
            LOG.trace("Configuration ID mismatch for proposal: current_config:{} proposal:{}", proposalMessage);
            return;
        }

        final AtomicInteger proposalsReceived = votesPerProposal.computeIfAbsent(proposalMessage.getHostsList(),
                                                                            (k) -> new AtomicInteger(0));
        final int count = proposalsReceived.incrementAndGet();
        final double F = ((double) membershipSize) / 3.0; // Fast Paxos resiliency.

        if (count >= (membershipSize - F)) {
            // We have a successful proposal. Consume it.
            decideViewChange(proposalMessage.getHostsList()
                                            .stream()
                                            .map(HostAndPort::fromString)
                                            .collect(Collectors.toList()));
        }
    }


    /**
     * This is invoked by Consensus modules when they arrive at a decision.
     *
     * Any node that is not in the membership list will be added to the cluster,
     * and any node that is currently in the membership list will be removed from it.
     */
    @ExperimentalApi
    void decideViewChange(final List<HostAndPort> proposal) {
        final List<NodeStatusChange> statusChanges = new ArrayList<>(proposal.size());
        for (final HostAndPort node : proposal) {
            final boolean isPresent = membershipView.isHostPresent(node);
            // If the node is already in the ring, remove it. Else, add it.
            // XXX: Maybe there's a cleaner way to do this in the future because
            // this ties us to just two states a node can be in.
            if (isPresent) {
                membershipView.ringDelete(node);
                statusChanges.add(new NodeStatusChange(node, LinkStatus.DOWN, metadataManager.get(node)));
                metadataManager.removeNode(node);
            }
            else {
                assert joinerUuid.containsKey(node);
                membershipView.ringAdd(node, joinerUuid.get(node));
                statusChanges.add(new NodeStatusChange(node, LinkStatus.UP, metadataManager.get(node)));
            }
        }

        // Publish an event to the listeners.
        subscriptions.get(ClusterEvents.VIEW_CHANGE)
                .forEach(cb -> cb.accept(statusChanges));

        // Clear data structures for the next round.
        watermarkBuffer.clear();
        votesPerProposal.clear();
        announcedProposal = false;

        // Inform LinkFailureDetector about membership change
        linkFailureDetectorRunner.updateMembership(membershipView.getMonitoreesOf(myAddr));

        // This should yield the new configuration.
        final MembershipView.Configuration configuration = membershipView.getConfiguration();

        assert configuration.hostAndPorts.size() > 0;
        assert configuration.uuids.size() > 0;

        final JoinResponse response = JoinResponse.newBuilder()
                .setSender(this.myAddr.toString())
                .setStatusCode(JoinStatusCode.SAFE_TO_JOIN)
                .setConfigurationId(configuration.getConfigurationId())
                .addAllHosts(configuration.hostAndPorts
                        .stream()
                        .map(HostAndPort::toString)
                        .collect(Collectors.toList()))
                .addAllIdentifiers(configuration.uuids
                        .stream()
                        .map(UUID::toString)
                        .collect(Collectors.toList()))
                .build();

        // Send out responses to all the nodes waiting to join.
        for (final HostAndPort node: proposal) {
            if (joinersToRespondTo.contains(node)) {
                scheduledExecutorService.execute(() -> rpcClient.sendJoinConfirmation(node, response));
            }
        }
    }

    /**
     * Invoked by monitors of a node for failure detection.
     */
    void processProbeMessage(final ProbeMessage probeMessage,
                             final StreamObserver<ProbeResponse> probeResponseObserver) {
        linkFailureDetectorRunner.handleProbeMessage(probeMessage, probeResponseObserver);
    }


    /**
     * Invoked by subscribers waiting for event notifications.
     * @param event Cluster event to subscribe to
     * @param callback Callback to be executed when {@code event} occurs.
     */
    void registerSubscription(final ClusterEvents event,
                              final Consumer<List<NodeStatusChange>> callback) {
        subscriptions.get(event).add(callback);
    }


    /**
     * This is a notification from a local link failure detector at a monitor. This changes
     * the status of the edge between the monitor and the monitoree to DOWN.
     *
     * @param monitoree The monitoree that has failed.
     */
    private void linkFailureNotification(final HostAndPort monitoree) {
        // TODO: This should execute on the grpc-server thread.
        scheduledExecutorService.execute(() -> {
                final long configurationId = membershipView.getCurrentConfigurationId();
                if (LOG.isDebugEnabled()) {
                    final int size = membershipView.getRing(0).size();
                    LOG.debug("Announcing LinkFail event {monitoree:{}, monitor:{}, config:{}, size:{}}",
                            monitoree, myAddr, configurationId, size);
                }
                // Note: setUuid is deliberately missing here because it does not affect leaves.
                final LinkUpdateMessage.Builder msgTemplate = LinkUpdateMessage.newBuilder()
                        .setLinkSrc(myAddr.toString())
                        .setLinkDst(monitoree.toString())
                        .setLinkStatus(LinkStatus.DOWN)
                        .setConfigurationId(configurationId);
                membershipView.getRingNumbers(myAddr, monitoree)
                        .forEach(i -> enqueueLinkUpdateMessage(msgTemplate.setRingNumber(i).build()));
            }
        );
    }


    /**
     * Gets the list of hosts currently in the membership view.
     *
     * @return list of hosts in the membership view
     */
    List<HostAndPort> getMembershipView() {
        return membershipView.getRing(0);
    }


    /**
     * If proposal logging is enabled, return the log of proposals made by a node
     *
     * @return Ordered log of view change proposals made by this service
     */
    List<Set<HostAndPort>> getProposalLog() {
        return Collections.unmodifiableList(logProposalList);
    }


    /**
     * Shuts down all the executors.
     */
    void shutdown() throws InterruptedException {
        scheduledExecutorService.shutdownNow();
        rpcClient.shutdown();
    }

    /**
     * Queues a LinkUpdateMessage to be broadcasted after potentially being batched.
     *
     * @param msg the LinkUpdateMessage to be broadcasted
     */
    private void enqueueLinkUpdateMessage(final LinkUpdateMessage msg) {
        batchSchedulerLock.lock();
        try {
            lastEnqueueTimestamp = System.currentTimeMillis();
            sendQueue.add(msg);
        }
        finally {
            batchSchedulerLock.unlock();
        }
    }

    /**
     * Formats a proposal or a view change for application subscriptions.
     */
    private List<NodeStatusChange> createNodeStatusChangeList(final Set<HostAndPort> proposal) {
        final List<NodeStatusChange> list = new ArrayList<>(proposal.size());
        for (final HostAndPort node: proposal) {
            final LinkStatus status = membershipView.isHostPresent(node) ? LinkStatus.UP : LinkStatus.DOWN;
            list.add(new NodeStatusChange(node, status, metadataManager.get(node)));
        }
        return list;
    }

    /**
     * Batches outgoing LinkUpdateMessages into a single BatchLinkUpdateMessage.
     */
    private class LinkUpdateBatcher implements Runnable {
        private static final int BATCH_WINDOW_IN_MS = 100;

        @Override
        public void run() {
            batchSchedulerLock.lock();
            try {
                // Wait one BATCH_WINDOW_IN_MS since last add before sending out
                if (sendQueue.size() > 0 && lastEnqueueTimestamp > 0
                        && (System.currentTimeMillis() - lastEnqueueTimestamp) > BATCH_WINDOW_IN_MS) {
                    LOG.trace("{}'s scheduler is sending out {} messages", myAddr, sendQueue.size());
                    final ArrayList<LinkUpdateMessage> messages = new ArrayList<>(sendQueue.size());
                    sendQueue.drainTo(messages);
                    final BatchedLinkUpdateMessage batched = BatchedLinkUpdateMessage.newBuilder()
                            .setSender(myAddr.toString())
                            .addAllMessages(messages)
                            .build();
                    // TODO: for the time being, we perform an all-to-all broadcast. Will be replaced with gossip.
                    broadcaster.broadcast(membershipView.getRing(0), batched);
                }
            }
            finally {
                batchSchedulerLock.unlock();
            }
        }
    }
}