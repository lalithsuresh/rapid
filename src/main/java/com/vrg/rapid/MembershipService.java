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
import com.google.protobuf.ByteString;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.Executor;
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
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final HostAndPort myAddr;
    private final boolean logProposals;
    private final IBroadcaster broadcaster;
    private final Map<HostAndPort, BlockingQueue<StreamObserver<JoinResponse>>> joinResponseCallbacks;
    private final Map<HostAndPort, UUID> joinerUuid = new ConcurrentHashMap<>();
    private final List<Set<HostAndPort>> logProposalList = new ArrayList<>();
    private final Executor executor = Executors.newSingleThreadScheduledExecutor();

    // Fields used by batching logic.
    @GuardedBy("batchSchedulerLock")
    private long lastEnqueueTimestamp = -1;    // Timestamp
    @GuardedBy("batchSchedulerLock")
    private final LinkedBlockingQueue<LinkUpdateMessage> sendQueue = new LinkedBlockingQueue<>();
    private final Lock batchSchedulerLock = new ReentrantLock();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);


    public static class Builder {
        private final MembershipView membershipView;
        private final WatermarkBuffer watermarkBuffer;
        private final HostAndPort myAddr;
        private final IBroadcaster broadcaster;
        private boolean logProposals;

        public Builder(final HostAndPort myAddr,
                       final WatermarkBuffer watermarkBuffer,
                       final MembershipView membershipView) {
            this.myAddr = Objects.requireNonNull(myAddr);
            this.watermarkBuffer = Objects.requireNonNull(watermarkBuffer);
            this.membershipView = Objects.requireNonNull(membershipView);
            final RpcClient rpcClient = new RpcClient(myAddr);
            this.broadcaster = new UnicastToAllBroadcaster(rpcClient);
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
        this.logProposals = builder.logProposals;
        this.broadcaster = builder.broadcaster;
        this.joinResponseCallbacks = new ConcurrentHashMap<>();
        this.scheduledExecutorService.scheduleAtFixedRate(new LinkUpdateBroadcastScheduler(),
                0, 100, TimeUnit.MILLISECONDS);
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
                builder.addAllHosts(membershipView.expectedMonitorsOf(joiningHost)
                        .stream()
                        .map(e -> ByteString.copyFromUtf8(e.toString()))
                        .collect(Collectors.toList()));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    /**
     * Invoked by a joining node at its (future) monitors. They perform any failure checking
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
                // TODO: insert some health checks between monitor and client
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

                // TODO: for the time being, perform an all-to-all broadcast. We will later replace this with gossip.
                batchSchedulerLock.lock();
                try {
                    lastEnqueueTimestamp = System.currentTimeMillis();
                    sendQueue.add(msg);
                }
                finally {
                    batchSchedulerLock.unlock();
                }

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
                LOG.trace("LinkUpdateMessage received for {sender:{}, receiver:{}, config:{}, size:{}}",
                        messageBatch.getSender(), myAddr,
                        request.getConfigurationId(), membershipView.getRing(0).size());

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
                if (request.getLinkStatus().equals(LinkStatus.DOWN) && !membershipView.isHostPresent(destination)) {
                    LOG.trace("LinkUpdateMessage with status DOWN received for node {} already in configuration {} ",
                            request.getLinkDst(), currentConfigurationId);
                    continue;
                }

                joinerUuid.put(destination, UUID.fromString(request.getUuid()));
                validMessages.add(request);
            }

            // We now batch apply all the valid messages innto the watermark buffer, obtaining
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

    private List<HostAndPort> proposedViewChange(final LinkUpdateMessage msg) {
        return watermarkBuffer.aggregateForProposal(msg);
    }

    List<Set<HostAndPort>> getProposalLog() {
        return Collections.unmodifiableList(logProposalList);
    }

    List<HostAndPort> getMembershipView() {
        return membershipView.getRing(0);
    }

    void shutdown() {
        scheduledExecutorService.shutdown();
    }

    private class LinkUpdateBroadcastScheduler implements Runnable {
        private static final int BATCH_WINDOW_IN_MS = 100;

        @Override
        public void run() {
            batchSchedulerLock.lock();
            try {
                // One second since last add
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
                    broadcaster.broadcast(membershipView.getRing(0), batched);
                }
            }
            finally {
                batchSchedulerLock.unlock();
            }
        }
    }
}