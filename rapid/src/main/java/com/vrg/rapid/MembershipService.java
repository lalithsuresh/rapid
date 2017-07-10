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
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
    private static int FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 1000;
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final HostAndPort myAddr;
    private final IBroadcaster broadcaster;
    private final Map<HostAndPort, LinkedBlockingDeque<StreamObserver<JoinResponse>>> joinersToRespondTo =
            new HashMap<>();
    private final Map<HostAndPort, NodeId> joinerUuid = new HashMap<>();
    private final Map<HostAndPort, Metadata> joinerMetadata = new HashMap<>();
    private final LinkFailureDetectorRunner linkFailureDetectorRunner;
    private final RpcClient rpcClient;
    private final MetadataManager metadataManager;

    // Event subscriptions
    private final Map<ClusterEvents, List<Consumer<List<NodeStatusChange>>>> subscriptions;

    // Fields used by batching logic.
    @GuardedBy("batchSchedulerLock")
    private long lastEnqueueTimestamp = -1;    // Timestamp
    @GuardedBy("batchSchedulerLock")
    private final LinkedBlockingQueue<LinkUpdateMessage> sendQueue = new LinkedBlockingQueue<>();
    private final Lock batchSchedulerLock = new ReentrantLock();
    private final ScheduledExecutorService backgroundTasksExecutor;
    private final ScheduledFuture<?> linkUpdateBatcherJob;
    private final ScheduledFuture<?> failureDetectorJob;
    private final SharedResources sharedResources;

    // Fields used by consensus protocol
    private final Map<List<String>, AtomicInteger> votesPerProposal = new HashMap<>();
    private final Set<String> votesReceived = new HashSet<>(); // Should be a bitset
    private boolean announcedProposal = false;
    private final Object membershipUpdateLock = new Object();

    static class Builder {
        private final MembershipView membershipView;
        private final WatermarkBuffer watermarkBuffer;
        private final HostAndPort myAddr;
        private final SharedResources sharedResources;
        private Map<String, Metadata> metadata = Collections.emptyMap();
        @Nullable private ILinkFailureDetector linkFailureDetector = null;
        @Nullable private RpcClient rpcClient = null;
        @Nullable private Map<ClusterEvents, List<Consumer<List<NodeStatusChange>>>> subscriptions = null;

        Builder(final HostAndPort myAddr,
                final WatermarkBuffer watermarkBuffer,
                final MembershipView membershipView,
                final SharedResources sharedResources) {
            this.myAddr = Objects.requireNonNull(myAddr);
            this.watermarkBuffer = Objects.requireNonNull(watermarkBuffer);
            this.membershipView = Objects.requireNonNull(membershipView);
            this.sharedResources = sharedResources;
        }

        Builder setMetadata(final Map<String, Metadata> metadata) {
            this.metadata = metadata;
            return this;
        }

        Builder setLinkFailureDetector(final ILinkFailureDetector linkFailureDetector) {
            this.linkFailureDetector = linkFailureDetector;
            return this;
        }

        Builder setRpcClient(final RpcClient rpcClient) {
            this.rpcClient = rpcClient;
            return this;
        }

        Builder setSubscriptions(final Map<ClusterEvents, List<Consumer<List<NodeStatusChange>>>> subscriptions) {
            this.subscriptions = subscriptions;
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
        this.sharedResources = builder.sharedResources;
        this.metadataManager = new MetadataManager();
        this.metadataManager.addMetadata(builder.metadata);
        this.rpcClient = builder.rpcClient != null ? builder.rpcClient : new RpcClient(myAddr);
        this.broadcaster = new UnicastToAllBroadcaster(rpcClient);
        if (builder.subscriptions == null) {
            this.subscriptions = new HashMap<>(ClusterEvents.values().length); // One for each event.
            Arrays.stream(ClusterEvents.values()).forEach(event ->
                    this.subscriptions.put(event, new ArrayList<>(1)));
        }
        else {
            this.subscriptions = builder.subscriptions;
            Arrays.stream(ClusterEvents.values()).forEach(event ->
                    this.subscriptions.computeIfAbsent(event, (k) -> new ArrayList<>(1)));
        }

        // Schedule background jobs
        this.backgroundTasksExecutor = builder.sharedResources.getScheduledTasksExecutor();
        linkUpdateBatcherJob = this.backgroundTasksExecutor.scheduleAtFixedRate(new LinkUpdateBatcher(),
                0, BATCHING_WINDOW_IN_MS, TimeUnit.MILLISECONDS);

        this.broadcaster.setMembership(membershipView.getRing(0));
        // this::linkFailureNotification is invoked by the failure detector whenever an edge
        // to a monitor is marked faulty.
        final ILinkFailureDetector fd  = builder.linkFailureDetector != null
                                        ? builder.linkFailureDetector
                                        : new PingPongFailureDetector(myAddr, this.rpcClient);
        this.linkFailureDetectorRunner = new LinkFailureDetectorRunner(fd);
        this.linkFailureDetectorRunner.registerSubscription(this::linkFailureNotification);

        // This primes the link failure detector with the initial set of monitorees.
        linkFailureDetectorRunner.updateMembership(membershipView.getMonitoreesOf(myAddr),
                                                   membershipView.getCurrentConfigurationId());
        failureDetectorJob = this.backgroundTasksExecutor.scheduleAtFixedRate(linkFailureDetectorRunner,
                FAILURE_DETECTOR_INITIAL_DELAY_IN_MS, FAILURE_DETECTOR_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);

        // Execute all VIEW_CHANGE callbacks. This informs applications that a start/join has successfully completed.
        final List<NodeStatusChange> nodeStatusChanges = getInitialViewChange();
        subscriptions.get(ClusterEvents.VIEW_CHANGE).forEach(cb -> cb.accept(nodeStatusChanges));
    }

    /**
     * This is invoked by a new node joining the network at a seed node.
     * The seed responds with the current configuration ID and a list of monitors
     * for the joiner, who then moves on to phase 2 of the protocol with its monitors.
     */
    void processJoinMessage(final JoinMessage joinMessage,
                            final StreamObserver<JoinResponse> responseObserver) {
        final HostAndPort joiningHost = HostAndPort.fromString(joinMessage.getSender());
        final JoinStatusCode statusCode = membershipView.isSafeToJoin(joiningHost, joinMessage.getNodeId());
        final JoinResponse.Builder builder = JoinResponse.newBuilder()
                .setSender(this.myAddr.toString())
                .setConfigurationId(membershipView.getCurrentConfigurationId())
                .setStatusCode(statusCode);
        LOG.trace("Join at seed for {seed:{}, sender:{}, config:{}, size:{}}",
                myAddr, joinMessage.getSender(),
                membershipView.getCurrentConfigurationId(), membershipView.getMembershipSize());
        if (statusCode.equals(JoinStatusCode.SAFE_TO_JOIN)
                || statusCode.equals(JoinStatusCode.HOSTNAME_ALREADY_IN_RING)) {
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
                                    final StreamObserver<JoinResponse> responseObserver) {
        final long currentConfiguration = membershipView.getCurrentConfigurationId();
        if (currentConfiguration == joinMessage.getConfigurationId()) {
            LOG.trace("Enqueuing SAFE_TO_JOIN for {sender:{}, monitor:{}, config:{}, size:{}}",
                    joinMessage.getSender(), myAddr,
                    currentConfiguration, membershipView.getMembershipSize());

            joinersToRespondTo.computeIfAbsent(HostAndPort.fromString(joinMessage.getSender()),
                    (k) -> new LinkedBlockingDeque<>()).add(responseObserver);

            final LinkUpdateMessage msg = LinkUpdateMessage.newBuilder()
                    .setLinkSrc(this.myAddr.toString())
                    .setLinkDst(joinMessage.getSender())
                    .setLinkStatus(LinkStatus.UP)
                    .setConfigurationId(currentConfiguration)
                    .setNodeId(joinMessage.getNodeId())
                    .addAllRingNumber(joinMessage.getRingNumberList())
                    .setMetadata(joinMessage.getMetadata())
                    .build();
            enqueueLinkUpdateMessage(msg);
        } else {
            // This handles the corner case where the configuration changed between phase 1 and phase 2
            // of the joining node's bootstrap. It should attempt to rejoin the network.
            final MembershipView.Configuration configuration = membershipView.getConfiguration();
            LOG.info("Wrong configuration for {sender:{}, monitor:{}, config:{}, size:{}}",
                    joinMessage.getSender(), myAddr,
                    currentConfiguration, membershipView.getMembershipSize());
            JoinResponse.Builder responseBuilder = JoinResponse.newBuilder()
                    .setSender(this.myAddr.toString())
                    .setConfigurationId(configuration.getConfigurationId());
            if (membershipView.isHostPresent(HostAndPort.fromString(joinMessage.getSender()))
                    && membershipView.isIdentifierPresent(joinMessage.getNodeId())) {

                // Race condition where a monitor already crossed H messages for the joiner and changed
                // the configuration, but the JoinPhase2 messages show up at the monitor
                // after it has already added the joiner. In this case, we simply
                // tell the sender that they're safe to join.
                responseBuilder = responseBuilder.setStatusCode(JoinStatusCode.SAFE_TO_JOIN)
                        .addAllHosts(configuration.hostAndPorts
                                .stream()
                                .map(HostAndPort::toString)
                                .collect(Collectors.toList()))
                        .addAllIdentifiers(configuration.nodeIds);
            } else {
                responseBuilder = responseBuilder.setStatusCode(JoinStatusCode.CONFIG_CHANGED);
                LOG.info("Returning CONFIG_CHANGED for {sender:{}, monitor:{}, config:{}, size:{}}",
                        joinMessage.getSender(), myAddr,
                        configuration.getConfigurationId(), configuration.hostAndPorts.size());
            }
            responseObserver.onNext(responseBuilder.build()); // new config
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
        final long currentConfigurationId = membershipView.getCurrentConfigurationId();
        final int membershipSize = membershipView.getMembershipSize();
        final Set<HostAndPort> proposal = messageBatch.getMessagesList().stream()
                // First, we filter out invalid messages that violate membership invariants.
                .filter(msg -> filterLinkUpdateMessages(messageBatch, msg, membershipSize, currentConfigurationId))
                // We then apply all the valid messages into our condition detector to obtain a view change proposal
                .map(watermarkBuffer::aggregateForProposal)
                .flatMap(List::stream)
                .collect(Collectors.toSet());

        // Lastly, we apply implicit detections
        proposal.addAll(watermarkBuffer.invalidateFailingLinks(membershipView));

        // If we have a proposal for this stage, start an instance of consensus on it.
        if (proposal.size() != 0) {
            LOG.debug("Node {} has a proposal of size {}: {}", myAddr, proposal.size(), proposal);
            announcedProposal = true;

            if (subscriptions.containsKey(ClusterEvents.VIEW_CHANGE_PROPOSAL)) {
                final List<NodeStatusChange> result = createNodeStatusChangeList(proposal);
                // Inform subscribers that a proposal has been announced.
                subscriptions.get(ClusterEvents.VIEW_CHANGE_PROPOSAL).forEach(cb -> cb.accept(result));
            }

            final ConsensusProposal proposalMessage = ConsensusProposal.newBuilder()
                    .setConfigurationId(currentConfigurationId)
                    .addAllHosts(proposal
                            .stream()
                            .map(HostAndPort::toString)
                            .sorted()
                            .collect(Collectors.toList()))
                    .setSender(myAddr.toString())
                    .build();
            broadcaster.broadcast(proposalMessage);
        }
    }


    /**
     * Receives proposal for the one-step consensus (essentially phase 2 of Fast Paxos).
     *
     * XXX: Implement recovery for the extremely rare possibility of conflicting proposals.
     *
     */
    void processConsensusProposal(final ConsensusProposal proposalMessage) {
        final long currentConfigurationId = membershipView.getCurrentConfigurationId();
        final long membershipSize = membershipView.getMembershipSize();

        if (proposalMessage.getConfigurationId() != currentConfigurationId) {
            LOG.trace("Configuration ID mismatch for proposal: current_config:{} proposal:{}", currentConfigurationId,
                      proposalMessage);
            return;
        }

        if (votesReceived.contains(proposalMessage.getSender())) {
            return;
        }
        votesReceived.add(proposalMessage.getSender());
        final AtomicInteger proposalsReceived = votesPerProposal.computeIfAbsent(proposalMessage.getHostsList(),
                                                                            (k) -> new AtomicInteger(0));
        final int count = proposalsReceived.incrementAndGet();
        final int F = (int) Math.floor((membershipSize - 1) / 4.0); // Fast Paxos resiliency.
        if (votesReceived.size() >= membershipSize - F) {
            if (count >= membershipSize - F) {
                LOG.trace("{} has decided on a view change: {}", myAddr, proposalMessage.getHostsList());
                // We have a successful proposal. Consume it.
                decideViewChange(proposalMessage.getHostsList()
                                                .stream()
                                                .map(HostAndPort::fromString)
                                                .collect(Collectors.toList()));
            }
            else {
                // Extremely rare scenario. Complain to the application.
                List<String> proposalWithMostVotes = Collections.emptyList();
                int min = Integer.MIN_VALUE;
                for (final Map.Entry<List<String>, AtomicInteger> entry: votesPerProposal.entrySet()) {
                    if (entry.getValue().get() > min) {
                        proposalWithMostVotes = entry.getKey();
                        min = entry.getValue().get();
                    }
                }
                final Set<HostAndPort> toSet = proposalWithMostVotes.stream()
                                                    .map(HostAndPort::fromString).collect(Collectors.toSet());
                subscriptions.get(ClusterEvents.VIEW_CHANGE_ONE_STEP_FAILED)
                        .forEach(cb -> cb.accept(createNodeStatusChangeList(toSet)));
            }
        }
    }


    /**
     * This is invoked by Consensus modules when they arrive at a decision.
     *
     * Any node that is not in the membership list will be added to the cluster,
     * and any node that is currently in the membership list will be removed from it.
     */
    private void decideViewChange(final List<HostAndPort> proposal) {
        final List<NodeStatusChange> statusChanges = new ArrayList<>(proposal.size());
        synchronized (membershipUpdateLock) {
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
                    final NodeId nodeId = joinerUuid.remove(node);
                    membershipView.ringAdd(node, nodeId);
                    final Metadata metadata = joinerMetadata.remove(node);
                    if (metadata.getMetadataCount() > 0) {
                        metadataManager.addMetadata(Collections.singletonMap(node.toString(), metadata));
                    }
                    statusChanges.add(new NodeStatusChange(node, LinkStatus.UP, metadata));
                }
            }
        }

        // Publish an event to the listeners.
        subscriptions.get(ClusterEvents.VIEW_CHANGE)
                .forEach(cb -> cb.accept(statusChanges));

        // Clear data structures for the next round.
        watermarkBuffer.clear();
        votesPerProposal.clear();
        votesReceived.clear();
        announcedProposal = false;

        broadcaster.setMembership(membershipView.getRing(0));

        // This should yield the new configuration.
        final MembershipView.Configuration configuration = membershipView.getConfiguration();

        // Inform LinkFailureDetector about membership change
        if (membershipView.isHostPresent(myAddr)) {
            final List<HostAndPort> newMonitorees = membershipView.getMonitoreesOf(myAddr);
            linkFailureDetectorRunner.updateMembership(newMonitorees, configuration.getConfigurationId());
        }
        else {
            // We need to gracefully exit by calling a user handler and invalidating
            // the current session.
            LOG.trace("{} got kicked out and is shutting down.", myAddr);
            linkFailureDetectorRunner.updateMembership(Collections.emptyList(), configuration.getConfigurationId());
            return;
        }

        assert configuration.hostAndPorts.size() > 0;
        assert configuration.nodeIds.size() > 0;

        final JoinResponse response = JoinResponse.newBuilder()
                .setSender(this.myAddr.toString())
                .setStatusCode(JoinStatusCode.SAFE_TO_JOIN)
                .setConfigurationId(configuration.getConfigurationId())
                .addAllHosts(configuration.hostAndPorts
                        .stream()
                        .map(HostAndPort::toString)
                        .collect(Collectors.toList()))
                .addAllIdentifiers(configuration.nodeIds)
                .putAllClusterMetadata(metadataManager.getAllMetadata())
                .build();

        // Send out responses to all the nodes waiting to join.
        for (final HostAndPort node: proposal) {
            if (joinersToRespondTo.containsKey(node)) {
                backgroundTasksExecutor.execute(
                    () -> {
                        joinersToRespondTo.get(node).forEach(observer -> {
                            try {
                                observer.onNext(response);
                                observer.onCompleted();
                            } catch (final StatusRuntimeException e) {
                                LOG.warn("{} got a StatusRuntimeException {}", myAddr, e.getLocalizedMessage());
                            }
                        });
                        joinersToRespondTo.remove(node);
                    }
                );
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
    private void linkFailureNotification(final HostAndPort monitoree, final long configurationId) {
        sharedResources.getProtocolExecutor().execute(() -> {
                if (configurationId != membershipView.getCurrentConfigurationId()) {
                    LOG.info("Ignoring failure notification from old configuration" +
                                    " {monitoree:{}, monitor:{}, config:{}, oldConfiguration:{}}",
                            monitoree, myAddr, membershipView.getCurrentConfigurationId(), configurationId);
                    return;
                }
                if (LOG.isDebugEnabled()) {
                    final int size = membershipView.getMembershipSize();
                    LOG.debug("Announcing LinkFail event {monitoree:{}, monitor:{}, config:{}, size:{}}",
                            monitoree, myAddr, configurationId, size);
                }
                // Note: setUuid is deliberately missing here because it does not affect leaves.
                final LinkUpdateMessage msg = LinkUpdateMessage.newBuilder()
                        .setLinkSrc(myAddr.toString())
                        .setLinkDst(monitoree.toString())
                        .setLinkStatus(LinkStatus.DOWN)
                        .addAllRingNumber(membershipView.getRingNumbers(myAddr, monitoree))
                        .setConfigurationId(configurationId)
                        .build();
                enqueueLinkUpdateMessage(msg);
            }
        );
    }


    /**
     * Gets the list of hosts currently in the membership view.
     *
     * @return list of hosts in the membership view
     */
    List<HostAndPort> getMembershipView() {
        synchronized (membershipUpdateLock) {
            return membershipView.getRing(0);
        }
    }

    /**
     * Gets the list of hosts currently in the membership view.
     *
     * @return list of hosts in the membership view
     */
    int getMembershipSize() {
        synchronized (membershipUpdateLock) {
            return membershipView.getMembershipSize();
        }
    }


    /**
     * Gets the list of hosts currently in the membership view.
     *
     * @return list of hosts in the membership view
     */
    Map<String, Metadata> getMetadata() {
        synchronized (membershipUpdateLock) {
            return metadataManager.getAllMetadata();
        }
    }

    /**
     * Shuts down all the executors.
     */
    void shutdown() {
        linkUpdateBatcherJob.cancel(true);
        failureDetectorJob.cancel(true);
        backgroundTasksExecutor.shutdownNow();
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
    private List<NodeStatusChange> createNodeStatusChangeList(final Collection<HostAndPort> proposal) {
        final List<NodeStatusChange> list = new ArrayList<>(proposal.size());
        for (final HostAndPort node: proposal) {
            final LinkStatus status = membershipView.isHostPresent(node) ? LinkStatus.DOWN : LinkStatus.UP;
            list.add(new NodeStatusChange(node, status, metadataManager.get(node)));
        }
        return list;
    }

    /**
     * Prepares a view change notification for a node that has just become part of a cluster. This is invoked when the
     * membership service is first initialized by a new node, which only happens on a Cluster.join() or Cluster.start().
     * Therefore, all LinkStatus values will be UP.
     */
    private List<NodeStatusChange> getInitialViewChange() {
        final List<NodeStatusChange> list = new ArrayList<>(membershipView.getMembershipSize());
        for (final HostAndPort node: membershipView.getRing(0)) {
            final LinkStatus status = LinkStatus.UP;
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
                    broadcaster.broadcast(batched);
                }
            }
            finally {
                batchSchedulerLock.unlock();
            }
        }
    }

    /**
     * A filter for removing invalid link update messages. These include messages that were for a
     * configuration that the current node is not a part of, and messages that violate the semantics
     * of a node being a part of a configuration.
     */
    private boolean filterLinkUpdateMessages(final BatchedLinkUpdateMessage batchedLinkUpdateMessage,
                                             final LinkUpdateMessage linkUpdateMessage,
                                             final int membershipSize,
                                             final long currentConfigurationId) {
        final HostAndPort destination = HostAndPort.fromString(linkUpdateMessage.getLinkDst());
        LOG.trace("LinkUpdateMessage received {sender:{}, receiver:{}, config:{}, size:{}, status:{}}",
                batchedLinkUpdateMessage.getSender(), myAddr,
                linkUpdateMessage.getConfigurationId(),
                membershipSize,
                linkUpdateMessage.getLinkStatus());

        if (currentConfigurationId != linkUpdateMessage.getConfigurationId()) {
            LOG.trace("LinkUpdateMessage for configuration {} received during configuration {}",
                    linkUpdateMessage.getConfigurationId(), currentConfigurationId);
            return false;
        }

        // The invariant we want to maintain is that a node can only go into the
        // membership set once and leave it once.
        if (linkUpdateMessage.getLinkStatus().equals(LinkStatus.UP)
                && membershipView.isHostPresent(destination)) {
            LOG.trace("LinkUpdateMessage with status UP received for node {} already in configuration {} ",
                    linkUpdateMessage.getLinkDst(), currentConfigurationId);
            return false;
        }
        if (linkUpdateMessage.getLinkStatus().equals(LinkStatus.DOWN)
                && !membershipView.isHostPresent(destination)) {
            LOG.trace("LinkUpdateMessage with status DOWN received for node {} already in configuration {} ",
                    linkUpdateMessage.getLinkDst(), currentConfigurationId);
            return false;
        }

        if (linkUpdateMessage.getLinkStatus() == LinkStatus.UP) {
            // Both the UUID and Metadata are saved only after the node is done being added.
            joinerUuid.put(destination, linkUpdateMessage.getNodeId());
            joinerMetadata.put(destination, linkUpdateMessage.getMetadata());
        }
        return true;
    }
}