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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.vrg.rapid.monitoring.ILinkFailureDetector;
import com.vrg.rapid.monitoring.PingPongFailureDetector;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/**
 * Membership server class that implements the Rapid protocol.
 */
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
    private final Map<HostAndPort, BlockingQueue<StreamObserver<JoinResponse>>> joinResponseCallbacks;
    private final Map<HostAndPort, UUID> joinerUuid = new ConcurrentHashMap<>();
    private final List<Set<HostAndPort>> logProposalList = new ArrayList<>();
    private final ExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final ILinkFailureDetector linkFailureDetector;
    private final RpcClient rpcClient;

    // Fields used by batching logic.
    @GuardedBy("batchSchedulerLock")
    private long lastEnqueueTimestamp = -1;    // Timestamp
    @GuardedBy("batchSchedulerLock")
    private final LinkedBlockingQueue<LinkUpdateMessage> sendQueue = new LinkedBlockingQueue<>();
    private final Lock batchSchedulerLock = new ReentrantLock();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);


    static class Builder {
        private final MembershipView membershipView;
        private final WatermarkBuffer watermarkBuffer;
        private final HostAndPort myAddr;
        private final IBroadcaster broadcaster;
        private final RpcClient rpcClient;
        private boolean logProposals;
        private ILinkFailureDetector linkFailureDetector;

        Builder(final HostAndPort myAddr,
                       final WatermarkBuffer watermarkBuffer,
                       final MembershipView membershipView) {
            this.myAddr = Objects.requireNonNull(myAddr);
            this.watermarkBuffer = Objects.requireNonNull(watermarkBuffer);
            this.membershipView = Objects.requireNonNull(membershipView);
            this.rpcClient = new RpcClient(myAddr);
            this.broadcaster = new UnicastToAllBroadcaster(rpcClient);
            this.linkFailureDetector = new PingPongFailureDetector(myAddr);
        }

        Builder setLogProposals(final boolean logProposals) {
            this.logProposals = logProposals;
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
        this.broadcaster = builder.broadcaster;
        this.joinResponseCallbacks = new ConcurrentHashMap<>();
        this.rpcClient = builder.rpcClient;
        this.linkFailureDetector = builder.linkFailureDetector;

        // Schedule background jobs
        this.scheduledExecutorService.scheduleAtFixedRate(new LinkUpdateBatcher(),
                0, BATCHING_WINDOW_IN_MS, TimeUnit.MILLISECONDS);

        // This primes the link failure detector with the initial membership set
        this.linkFailureDetector.onMembershipChange(membershipView.getMonitoreesOf(myAddr));
        this.scheduledExecutorService.scheduleAtFixedRate(new LinkFailureDetectorRunner(),
                FAILURE_DETECTOR_INITIAL_DELAY_IN_MS, FAILURE_DETECTOR_INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * This is invoked by a new node joining the network at a seed node.
     * The seed responds with the current configuration ID and a list of monitors
     * for the joiner, who then moves on to phase 2 of the protocol with its monitors.
     */
    void processJoinMessage(final JoinMessage joinMessage,
                            final StreamObserver<JoinResponse> responseObserver) {
        executor.execute(() -> {
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
                        .map(e -> ByteString.copyFromUtf8(e.toString()))
                        .collect(Collectors.toList()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    /**
     * Invoked by gatekeepers of a joining node. They perform any failure checking
     * required before propagating a LinkUpdateMessage with the status UP. After the watermarking
     * and consensus succeeds, the monitor informs the joiner about the new configuration it
     * is now a part of.
     */
    void processJoinPhaseTwoMessage(final JoinMessage joinMessage,
                                    final StreamObserver<JoinResponse> responseObserver) {
        executor.execute(() -> {
            final long currentConfiguration = membershipView.getCurrentConfigurationId();
            if (currentConfiguration == joinMessage.getConfigurationId()) {
                LOG.trace("Enqueuing SAFE_TO_JOIN for {sender:{}, monitor:{}, config:{}, size:{}}",
                        joinMessage.getSender(), myAddr,
                        currentConfiguration, membershipView.getRing(0).size());

                final LinkUpdateMessage msg = LinkUpdateMessage.newBuilder()
                        .setLinkSrc(this.myAddr.toString())
                        .setLinkDst(joinMessage.getSender())
                        .setLinkStatus(LinkStatus.UP)
                        .setConfigurationId(joinMessage.getConfigurationId())
                        .setUuid(joinMessage.getUuid())
                        .setRingNumber(joinMessage.getRingNumber())
                        .build();

                joinResponseCallbacks.computeIfAbsent(HostAndPort.fromString(joinMessage.getSender()),
                        (k) -> new LinkedBlockingQueue<>())
                        .add(responseObserver);
                enqueueLinkUpdateMessage(msg);
            } else {
                // This handles the corner case where the configuration changed between phase 1 and phase 2
                // of the joining node's bootstrap. It should attempt to rejoin the network.
                final MembershipView.Configuration configuration = membershipView.getConfiguration();

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
                                    .map(e -> ByteString.copyFromUtf8(e.toString()))
                                    .collect(Collectors.toList()))
                            .addAllIdentifiers(configuration.uuids
                                    .stream()
                                    .map(e -> ByteString.copyFromUtf8(e.toString()))
                                    .collect(Collectors.toList()));
                } else {
                    responseBuilder = responseBuilder.setStatusCode(JoinStatusCode.CONFIG_CHANGED);
                    LOG.trace("Returning CONFIG_CHANGED for {sender:{}, monitor:{}, config:{}, size:{}}",
                            joinMessage.getSender(), myAddr,
                            configuration.getConfigurationId(), configuration.hostAndPorts.size());
                }

                responseObserver.onNext(responseBuilder.build()); // new config
                responseObserver.onCompleted();
            }
        });
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
        executor.execute(() -> {
            Objects.requireNonNull(messageBatch);

            // We proceed in three parts. First, we collect the messages from the batch that
            // are valid and do not violate conditions of the ring. These are packed in to
            // validMessages and processed later.
            final List<LinkUpdateMessage> validMessages = new ArrayList<>(messageBatch.getMessagesList().size());
            for (final LinkUpdateMessage request: messageBatch.getMessagesList()) {
                final HostAndPort destination = HostAndPort.fromString(request.getLinkDst());
                LOG.trace("LinkUpdateMessage received {sender:{}, receiver:{}, config:{}, size:{}, status:{}}",
                        messageBatch.getSender(), myAddr,
                        request.getConfigurationId(),
                        membershipView.getRing(0).size(),
                        request.getLinkStatus());

                final long currentConfigurationId = membershipView.getCurrentConfigurationId();
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
                proposal.addAll(proposedViewChange(msg));
            }

            // If we have a proposal for this stage, start an instance of consensus on it.
            if (proposal.size() != 0) {
                if (logProposals) {
                    logProposalList.add(proposal);
                }

                // TODO: Initiate consensus here.

                // TODO: for now, we just apply the proposal directly.
                for (final HostAndPort node : proposal) {
                    final boolean isPresent = membershipView.isHostPresent(node);
                    // If the node is already in the ring, remove it. Else, add it.
                    // XXX: Maybe there's a cleaner way to do this in the future because
                    // this ties us to just two states a node can be in.
                    if (isPresent) {
                        membershipView.ringDelete(node);
                    }
                    else {
                        membershipView.ringAdd(node, joinerUuid.get(node));
                    }
                }

                // Inform LinkFailureDetector about membership change
                linkFailureDetector.onMembershipChange(membershipView.getMonitoreesOf(myAddr));

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
                                .map(e -> ByteString.copyFromUtf8(e.toString()))
                                .collect(Collectors.toList()))
                        .addAllIdentifiers(configuration.uuids
                                .stream()
                                .map(e -> ByteString.copyFromUtf8(e.toString()))
                                .collect(Collectors.toList()))
                        .build();

                // Send out responses to all the nodes waiting to join.
                for (final HostAndPort node: proposal) {
                    if (joinResponseCallbacks.containsKey(node)) {
                        joinResponseCallbacks.get(node).forEach(observer -> executor.execute(() -> {
                            try {
                                observer.onNext(response);
                                observer.onCompleted();
                            } catch (final StatusRuntimeException e) {
                                LOG.error("StatusRuntimeException of type {} for response JoinPhase2Response to {}",
                                          e.getStatus(), response.getSender());
                            }
                        }
                        ));
                        joinResponseCallbacks.remove(node);
                    }
                }
            }
        });
    }


    /**
     * Invoked by monitors of a node.
     *
     */
    void processProbeMessage(final ProbeMessage probeMessage,
                             final StreamObserver<ProbeResponse> probeResponseObserver) {
        linkFailureDetector.handleProbeMessage(probeMessage, probeResponseObserver);
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
    void shutdown() {
        executor.shutdown();
        scheduledExecutorService.shutdown();
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

    private List<HostAndPort> proposedViewChange(final LinkUpdateMessage msg) {
        return watermarkBuffer.aggregateForProposal(msg);
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
                    final List<HostAndPort> recipients = membershipView.getRing(0);
                    if (recipients.size() == 1 && recipients.get(0).equals(myAddr)) {
                        // Avoid RPC overhead during bootstrap.
                        executor.execute(() -> processLinkUpdateMessage(batched));
                        return;
                    }
                    // TODO: for the time being, we perform an all-to-all broadcast. Will be replaced with gossip.
                    broadcaster.broadcast(membershipView.getRing(0), batched);
                }
            }
            finally {
                batchSchedulerLock.unlock();
            }
        }
    }

    /**
     * Periodically executes a failure detector. In the future, the frequency of invoking this
     * function may be left to the LinkFailureDetector object itself.
     */
    private class LinkFailureDetectorRunner implements Runnable {

        @Override
        public void run() {
            /*
             * For every monitoree, first check if the link has failed. If not,
             * send out a probe request and handle the onSuccess and onFailure callbacks.
             */
            try {
                final Set<HostAndPort> monitorees = new HashSet<>(membershipView.getMonitoreesOf(myAddr));
                if (monitorees.size() == 0) {
                    return;
                }
                final List<ListenableFuture<ProbeResponse>> probes = new ArrayList<>();
                for (final HostAndPort monitoree : monitorees) {
                    if (!linkFailureDetector.hasFailed(monitoree)) {
                        // Node is up, so send it a probe and attach the callbacks.
                        final ProbeMessage message = linkFailureDetector.createProbe(monitoree);
                        final ListenableFuture<ProbeResponse> probeSend = rpcClient.sendProbeMessage(monitoree,
                                                                                                     message);
                        /*
                         * XXX: if we create a LinkFailureDetector per link, we can avoid the unnecessary object
                         * creation here and prepare these callbacks once per monitoree on every membership change.
                         * This requires some API changes to MembershipService in that users will supply a
                         * LinkFailureDetector factory as opposed to an instance of the failure detector object itself.
                         */
                        Futures.addCallback(probeSend, new FutureCallback<ProbeResponse>() {
                            @Override
                            public void onSuccess(@Nullable final ProbeResponse probeResponse) {
                                linkFailureDetector.handleProbeOnSuccess(probeResponse, monitoree);
                            }

                            @Override
                            public void onFailure(final Throwable throwable) {
                                linkFailureDetector.handleProbeOnFailure(throwable, monitoree);
                            }
                        });

                        probes.add(probeSend);
                    } else {
                        // A link has failed. Announce a LinkStatus.DOWN event.
                        executor.execute(() -> {
                            final long configurationId = membershipView.getCurrentConfigurationId();

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Announcing LinkFail event {monitoree:{}, monitor:{}, config:{}, size:{}}",
                                        monitoree, myAddr, configurationId, membershipView.getRing(0).size());
                            }

                            // Note: setUuid is deliberately missing here because it does not affect leaves.
                            final LinkUpdateMessage.Builder msgTemplate = LinkUpdateMessage.newBuilder()
                                    .setLinkSrc(myAddr.toString())
                                    .setLinkDst(monitoree.toString())
                                    .setLinkStatus(LinkStatus.DOWN)
                                    .setConfigurationId(configurationId);
                            membershipView.getRingNumbers(myAddr, monitoree)
                                          .forEach(i -> enqueueLinkUpdateMessage(
                                                  msgTemplate.setRingNumber(i)
                                                             .build()
                                                ));
                        });
                    }
                }

                // Failed requests will have their onFailure() events called. So it is okay to
                // only block for the successful ones here.
                Futures.successfulAsList(probes).get();
            }
            catch (final ExecutionException | StatusRuntimeException e) {
                LOG.error("Potential link failures: some probe messages have failed.");
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}