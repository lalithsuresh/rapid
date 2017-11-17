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
import com.google.common.util.concurrent.SettableFuture;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.PreJoinMessage;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;


/**
 * Membership server class that implements the Rapid protocol.
 *
 * Note: This class is not thread-safe yet. RpcServer.start() uses a single threaded messagingExecutor during the server
 * initialization to make sure that only a single thread runs the process* methods.
 *
 */
@NotThreadSafe
public final class MembershipService {
    private static final Logger LOG = LoggerFactory.getLogger(MembershipService.class);
    private static final int BATCHING_WINDOW_IN_MS = 100;
    private static final int DEFAULT_FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 0;
    static final int DEFAULT_FAILURE_DETECTOR_INTERVAL_IN_MS = 1000;
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final HostAndPort myAddr;
    private final IBroadcaster broadcaster;
    private final Map<HostAndPort, LinkedBlockingDeque<SettableFuture<RapidResponse>>> joinersToRespondTo =
            new HashMap<>();
    private final Map<HostAndPort, NodeId> joinerUuid = new HashMap<>();
    private final Map<HostAndPort, Metadata> joinerMetadata = new HashMap<>();
    private final IMessagingClient messagingClient;
    private final MetadataManager metadataManager;

    // Event subscriptions
    private final Map<ClusterEvents, List<BiConsumer<Long, List<NodeStatusChange>>>> subscriptions;

    //
    private FastPaxos fastPaxosInstance;

    // Fields used by batching logic.
    @GuardedBy("batchSchedulerLock")
    private long lastEnqueueTimestamp = -1;    // Timestamp
    @GuardedBy("batchSchedulerLock")
    private final LinkedBlockingQueue<LinkUpdateMessage> sendQueue = new LinkedBlockingQueue<>();
    private final Lock batchSchedulerLock = new ReentrantLock();
    private final ScheduledExecutorService backgroundTasksExecutor;
    private final ScheduledFuture<?> linkUpdateBatcherJob;
    private final List<ScheduledFuture<?>> failureDetectorJobs;
    private final SharedResources sharedResources;

    // Failure detector
    private final ILinkFailureDetectorFactory fdFactory;

    // Fields used by consensus protocol
    private boolean announcedProposal = false;
    private final Object membershipUpdateLock = new Object();
    private final ISettings settings;


    MembershipService(final HostAndPort myAddr, final WatermarkBuffer watermarkBuffer,
                      final MembershipView membershipView, final SharedResources sharedResources,
                      final ISettings settings, final IMessagingClient messagingClient,
                      final ILinkFailureDetectorFactory linkFailureDetector) {
        this(myAddr, watermarkBuffer, membershipView, sharedResources, settings, messagingClient, linkFailureDetector,
             Collections.emptyMap(), new EnumMap<>(ClusterEvents.class));
    }

    MembershipService(final HostAndPort myAddr, final WatermarkBuffer watermarkBuffer,
                      final MembershipView membershipView, final SharedResources sharedResources,
                      final ISettings settings, final IMessagingClient messagingClient,
                      final ILinkFailureDetectorFactory linkFailureDetector, final Map<String, Metadata> metadataMap,
                      final Map<ClusterEvents, List<BiConsumer<Long, List<NodeStatusChange>>>> subscriptions) {
        this.myAddr = myAddr;
        this.settings = settings;
        this.membershipView = membershipView;
        this.watermarkBuffer = watermarkBuffer;
        this.sharedResources = sharedResources;
        this.metadataManager = new MetadataManager();
        this.metadataManager.addMetadata(metadataMap);
        this.messagingClient = messagingClient;
        this.broadcaster = new UnicastToAllBroadcaster(messagingClient);
        this.subscriptions = subscriptions;
        this.fdFactory = linkFailureDetector;

        // Make sure there is an empty list for every enum type
        Arrays.stream(ClusterEvents.values()).forEach(event ->
                this.subscriptions.computeIfAbsent(event, k -> new ArrayList<>(0)));

        // Schedule background jobs
        this.backgroundTasksExecutor = sharedResources.getScheduledTasksExecutor();
        linkUpdateBatcherJob = this.backgroundTasksExecutor.scheduleAtFixedRate(new LinkUpdateBatcher(),
                0, BATCHING_WINDOW_IN_MS, TimeUnit.MILLISECONDS);

        this.broadcaster.setMembership(membershipView.getRing(0));
        // this::linkFailureNotification is invoked by the failure detector whenever an edge
        // to a monitor is marked faulty.
        this.failureDetectorJobs = new ArrayList<>();

        // Prepare consensus instance
        this.fastPaxosInstance = new FastPaxos(myAddr, membershipView.getCurrentConfigurationId(),
                                               membershipView.getMembershipSize(), this.broadcaster,
                                               this::decideViewChange);
        createFailureDetectorsForCurrentConfiguration();

        // Execute all VIEW_CHANGE callbacks. This informs applications that a start/join has successfully completed.
        final long configurationId = membershipView.getCurrentConfigurationId();
        final List<NodeStatusChange> nodeStatusChanges = getInitialViewChange();
        subscriptions.get(ClusterEvents.VIEW_CHANGE).forEach(cb -> cb.accept(configurationId, nodeStatusChanges));
    }

    /**
     * Entry point for all messages.
     */
    public ListenableFuture<RapidResponse> handleMessage(final RapidRequest msg) {
        switch (msg.getContentCase()) {
            case PREJOINMESSAGE:
                return handleMessage(msg.getPreJoinMessage());
            case JOINMESSAGE:
                return handleMessage(msg.getJoinMessage());
            case BATCHEDLINKUPDATEMESSAGE:
                return handleMessage(msg.getBatchedLinkUpdateMessage());
            case CONSENSUSPROPOSAL:
                return handleMessage(msg.getConsensusProposal());
            case PROBEMESSAGE:
                return handleMessage(msg.getProbeMessage());
            case CONTENT_NOT_SET:
            default:
                throw new RuntimeException();
        }
    }

    /**
     * This is invoked by a new node joining the network at a seed node.
     * The seed responds with the current configuration ID and a list of monitors
     * for the joiner, who then moves on to phase 2 of the protocol with its monitors.
     */
    private ListenableFuture<RapidResponse> handleMessage(final PreJoinMessage msg) {
        final SettableFuture<RapidResponse> future = SettableFuture.create();

        sharedResources.getProtocolExecutor().execute(() -> {
            final HostAndPort joiningHost = HostAndPort.fromString(msg.getSender());
            final JoinStatusCode statusCode = membershipView.isSafeToJoin(joiningHost, msg.getNodeId());
            final JoinResponse.Builder builder = JoinResponse.newBuilder()
                    .setSender(this.myAddr.toString())
                    .setConfigurationId(membershipView.getCurrentConfigurationId())
                    .setStatusCode(statusCode);
            LOG.trace("Join at seed for {seed:{}, sender:{}, config:{}, size:{}}",
                    myAddr, msg.getSender(),
                    membershipView.getCurrentConfigurationId(), membershipView.getMembershipSize());
            if (statusCode.equals(JoinStatusCode.SAFE_TO_JOIN)
                    || statusCode.equals(JoinStatusCode.HOSTNAME_ALREADY_IN_RING)) {
                // Return a list of monitors for the joiner to contact for phase 2 of the protocol
                builder.addAllHosts(membershipView.getExpectedMonitorsOf(joiningHost)
                        .stream()
                        .map(HostAndPort::toString)
                        .collect(Collectors.toList()));
            }
            future.set(RapidResponse.newBuilder().setJoinResponse(builder.build()).build());
        });
        return future;
    }

    /**
     * Invoked by gatekeepers of a joining node. They perform any failure checking
     * required before propagating a LinkUpdateMessage with the status UP. After the watermarking
     * and consensus succeeds, the monitor informs the joiner about the new configuration it
     * is now a part of.
     */
    private ListenableFuture<RapidResponse> handleMessage(final JoinMessage joinMessage) {
        final SettableFuture<RapidResponse> future = SettableFuture.create();

        sharedResources.getProtocolExecutor().execute(() -> {
            final long currentConfiguration = membershipView.getCurrentConfigurationId();
            if (currentConfiguration == joinMessage.getConfigurationId()) {
                LOG.trace("Enqueuing SAFE_TO_JOIN for {sender:{}, monitor:{}, config:{}, size:{}}",
                        joinMessage.getSender(), myAddr,
                        currentConfiguration, membershipView.getMembershipSize());

                joinersToRespondTo.computeIfAbsent(HostAndPort.fromString(joinMessage.getSender()),
                        k -> new LinkedBlockingDeque<>()).add(future);

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
                LOG.info("Wrong configuration for {sender:{}, monitor:{}, config:{}, myConfig:{}, size:{}}",
                        joinMessage.getSender(), myAddr, joinMessage.getConfigurationId(),
                        currentConfiguration, membershipView.getMembershipSize());
                JoinResponse.Builder responseBuilder = JoinResponse.newBuilder()
                        .setSender(this.myAddr.toString())
                        .setConfigurationId(configuration.getConfigurationId());
                if (membershipView.isHostPresent(HostAndPort.fromString(joinMessage.getSender()))
                        && membershipView.isIdentifierPresent(joinMessage.getNodeId())) {
                    LOG.info("Joining host already present : {sender:{}, monitor:{}, config:{}, myConfig:{}, size:{}}",
                            joinMessage.getSender(), myAddr, joinMessage.getConfigurationId(),
                            currentConfiguration, membershipView.getMembershipSize());
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
                future.set(RapidResponse.newBuilder().setJoinResponse(responseBuilder.build())
                                        .build()); // new configuration
            }
        });
        return future;
    }


    /**
     * This method receives link update events and delivers them to
     * the watermark buffer to check if it will return a valid
     * proposal.
     *
     * Link update messages that do not affect an ongoing proposal
     * needs to be dropped.
     */
    private ListenableFuture<RapidResponse> handleMessage(final BatchedLinkUpdateMessage messageBatch) {
        Objects.requireNonNull(messageBatch);
        final SettableFuture<RapidResponse> future = SettableFuture.create();

        sharedResources.getProtocolExecutor().execute(() -> {
            // We already have a proposal for this round
            // => we have initiated consensus and cannot go back on our proposal.
            if (announcedProposal) {
                future.set(null);
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
            if (!proposal.isEmpty()) {
                LOG.info("Node {} has a proposal of size {}: {}", myAddr, proposal.size(), proposal);
                announcedProposal = true;

                if (subscriptions.containsKey(ClusterEvents.VIEW_CHANGE_PROPOSAL)) {
                    final List<NodeStatusChange> result = createNodeStatusChangeList(proposal);
                    // Inform subscribers that a proposal has been announced.
                    subscriptions.get(ClusterEvents.VIEW_CHANGE_PROPOSAL)
                                 .forEach(cb -> cb.accept(currentConfigurationId, result));
                }
                fastPaxosInstance.propose(new ArrayList<>(proposal));
            }
            future.set(null);
        });
        return future;
    }


    /**
     * Receives proposal for the one-step consensus (essentially phase 2 of Fast Paxos).
     *
     * XXX: Implement recovery for the extremely rare possibility of conflicting proposals.
     *
     */
    ListenableFuture<RapidResponse> handleMessage(final ConsensusProposal proposalMessage) {
        final SettableFuture<RapidResponse> future = SettableFuture.create();
        sharedResources.getProtocolExecutor().execute(() -> {
            fastPaxosInstance.handleFastRoundProposal(proposalMessage);
            future.set(null);
        });
        return future;
    }


    /**
     * This is invoked by FastPaxos modules when they arrive at a decision.
     *
     * Any node that is not in the membership list will be added to the cluster,
     * and any node that is currently in the membership list will be removed from it.
     */
    private void decideViewChange(final List<HostAndPort> proposal) {
        // The first step is to disable our failure detectors in anticipation of new ones to be created.
        cancelFailureDetectorJobs();

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

        final long currentConfigurationId = membershipView.getCurrentConfigurationId();
        // Publish an event to the listeners.
        subscriptions.get(ClusterEvents.VIEW_CHANGE).forEach(cb -> cb.accept(currentConfigurationId, statusChanges));

        // Clear data structures for the next round.
        watermarkBuffer.clear();
        announcedProposal = false;
        fastPaxosInstance = new FastPaxos(myAddr, currentConfigurationId, membershipView.getMembershipSize(),
                                          broadcaster, this::decideViewChange);
        broadcaster.setMembership(membershipView.getRing(0));

        // Inform LinkFailureDetector about membership change
        if (membershipView.isHostPresent(myAddr)) {
            createFailureDetectorsForCurrentConfiguration();
        }
        else {
            // We need to gracefully exit by calling a user handler and invalidating
            // the current session.
            LOG.trace("{} got kicked out and is shutting down.", myAddr);
            subscriptions.get(ClusterEvents.KICKED).forEach(cb -> cb.accept(currentConfigurationId, statusChanges));
        }

        // Send new configuration to all nodes joining through us
        respondToJoiners(proposal);
    }

    /**
     * Invoked by monitors of a node for failure detection.
     */
    private ListenableFuture<RapidResponse> handleMessage(final ProbeMessage probeMessage) {
        LOG.trace("handleProbeMessage at {} from {}", myAddr, probeMessage.getSender());
        return Futures.immediateFuture(RapidResponse.newBuilder()
                .setProbeResponse(ProbeResponse.getDefaultInstance()).build());
    }


    /**
     * Invoked by subscribers waiting for event notifications.
     * @param event Cluster event to subscribe to
     * @param callback Callback to be executed when {@code event} occurs.
     */
    void registerSubscription(final ClusterEvents event,
                              final BiConsumer<Long, List<NodeStatusChange>> callback) {
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
        });
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
        failureDetectorJobs.forEach(k -> k.cancel(true));
        messagingClient.shutdown();
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
                if (!sendQueue.isEmpty() && lastEnqueueTimestamp > 0
                        && (System.currentTimeMillis() - lastEnqueueTimestamp) > BATCH_WINDOW_IN_MS) {
                    LOG.trace("{}'s scheduler is sending out {} messages", myAddr, sendQueue.size());
                    final ArrayList<LinkUpdateMessage> messages = new ArrayList<>(sendQueue.size());
                    final int numDrained = sendQueue.drainTo(messages);
                    assert numDrained > 0;
                    final BatchedLinkUpdateMessage batched = BatchedLinkUpdateMessage.newBuilder()
                            .setSender(myAddr.toString())
                            .addAllMessages(messages)
                            .build();
                    broadcaster.broadcast(RapidRequest.newBuilder()
                                                      .setBatchedLinkUpdateMessage(batched)
                                                      .build());
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

    /**
     * Invoked eventually by link failure detectors to notify MembershipService of failed nodes
     */
    private Runnable createNotifierForMonitoree(final HostAndPort monitoree) {
        return () -> linkFailureNotification(monitoree, membershipView.getCurrentConfigurationId());
    }

    /**
     * Creates and schedules failure detector instances based on the fdFactory instance.
     */
    private void createFailureDetectorsForCurrentConfiguration() {
        final List<ScheduledFuture<?>> jobs = membershipView.getMonitoreesOf(myAddr)
                .stream().map(monitoree -> backgroundTasksExecutor
                         .scheduleAtFixedRate(fdFactory.createInstance(monitoree,
                                createNotifierForMonitoree(monitoree)), // Runnable
                                DEFAULT_FAILURE_DETECTOR_INITIAL_DELAY_IN_MS,
                                settings.getFailureDetectorIntervalInMs(),
                                TimeUnit.MILLISECONDS))
                .collect(Collectors.toList());
        failureDetectorJobs.addAll(jobs);
    }

    /**
     * Cancel all running failure detector tasks
     */
    private void cancelFailureDetectorJobs() {
        failureDetectorJobs.forEach(future -> future.cancel(true));
    }

    /**
     * Respond with the current configuration to all nodes that attempted to join through this node.
     */
    private void respondToJoiners(final List<HostAndPort> proposal) {
        // This should yield the new configuration.
        final MembershipView.Configuration configuration = membershipView.getConfiguration();
        assert !configuration.hostAndPorts.isEmpty();
        assert !configuration.nodeIds.isEmpty();

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
                    () -> joinersToRespondTo.remove(node)
                                            .forEach(settableFuture -> settableFuture.set(RapidResponse.newBuilder()
                                                                                    .setJoinResponse(response).build()))
                );
            }
        }
    }

    interface ISettings {
        int getFailureDetectorIntervalInMs();
    }
}