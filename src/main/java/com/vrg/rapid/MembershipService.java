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
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessageWire;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;


/**
 * Membership server class that implements the Rapid protocol.
 */
public class MembershipService {
    private static final Logger LOG = LoggerFactory.getLogger(MembershipService.class);
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final HostAndPort myAddr;
    private final boolean logProposals;
    private final IBroadcaster broadcaster;
    private final List<List<WatermarkBuffer.Node>> logProposalList = new ArrayList<>();

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
    }

    /**
     * This is invoked by a new node joining the network at a seed node.
     * The seed responds with the current configuration ID and a list of monitors
     * for the joiner, who then moves on to phase 2 of the protocol with its monitors.
     */
    JoinResponse processJoinMessage(final JoinMessage joinMessage) {
        final HostAndPort joiningHost = HostAndPort.fromString(joinMessage.getSender());
        final UUID uuid = UUID.fromString(joinMessage.getUuid());
        final JoinStatusCode statusCode = membershipView.isSafeToJoin(joiningHost, uuid);
        final JoinResponse.Builder builder = JoinResponse.newBuilder()
                                                   .setSender(this.myAddr.toString())
                                                   .setConfigurationId(membershipView.getCurrentConfigurationId())
                                                   .setStatusCode(statusCode);

        if (statusCode.equals(JoinStatusCode.SAFE_TO_JOIN)) {
            // Return a list of monitors for the joiner to contact for phase 2 of the protocol
            builder.addAllHosts(membershipView.expectedMonitorsOf(joiningHost)
                    .stream()
                    .map(e -> ByteString.copyFromUtf8(e.toString()))
                    .collect(Collectors.toList()));
        }

        return builder.build();
    }

    /**
     * Invoked by a joining node at its (future) monitors. They perform any failure checking
     * required before propagating a LinkUpdateMessage with the status UP. After the watermarking
     * and consensus succeeds, the monitor informs the joiner about the new configuration it
     * is now a part of.
     */
    void processJoinPhaseTwoMessage(final JoinMessage joinMessage,
                                    final StreamObserver<JoinResponse> responseObserver) {
        final long currentConfiguration = membershipView.getCurrentConfigurationId();
        if (currentConfiguration == joinMessage.getConfigurationId()) {
            LOG.trace("Join phase 2 message received during configuration {}", currentConfiguration);
            // TODO: insert some health checks between monitor and client
            final LinkUpdateMessageWire msg = LinkUpdateMessageWire.newBuilder()
                    .setSender(this.myAddr.toString())
                    .setLinkSrc(this.myAddr.toString())
                    .setLinkDst(joinMessage.getSender())
                    .setLinkStatus(LinkStatus.UP)
                    .setConfigurationId(joinMessage.getConfigurationId())
                    .setUuid(joinMessage.getUuid())
                    .setRingNumber(joinMessage.getRingNumber())
                    .build();

            // TODO: for the time being, perform an all-to-all broadcast. We will later replace this with gossip.
            broadcaster.broadcast(membershipView.viewRing(0), msg);

            // TODO: the below code should only be executed after a successful membership change (using a callback).
            final MembershipView.Configuration configuration = membershipView.getConfiguration();

            assert configuration.hostAndPorts.size() > 0;
            assert configuration.uuids.size() > 0;

            final JoinResponse response = JoinResponse.newBuilder()
                    .setSender(this.myAddr.toString())
                    .setStatusCode(JoinStatusCode.SAFE_TO_JOIN)
                    .setConfigurationId(membershipView.getCurrentConfigurationId())
                    .addAllHosts(configuration.hostAndPorts
                            .stream()
                            .map(e -> ByteString.copyFromUtf8(e.toString()))
                            .collect(Collectors.toList()))
                    .addAllIdentifiers(configuration.uuids
                            .stream()
                            .map(e -> ByteString.copyFromUtf8(e.toString()))
                            .collect(Collectors.toList()))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        else {
            // This handles the corner case where the configuration changed between phase 1 and phase 2
            // of the joining node's bootstrap. It should attempt to rejoin the network.

            JoinResponse.Builder responseBuilder = JoinResponse.newBuilder()
                    .setSender(this.myAddr.toString())
                    .setConfigurationId(membershipView.getCurrentConfigurationId());

            if (membershipView.isHostPresent(HostAndPort.fromString(joinMessage.getSender()))
                && membershipView.isIdentifierPresent(UUID.fromString(joinMessage.getUuid()))) {
                final MembershipView.Configuration configuration = membershipView.getConfiguration();

                // Race condition where we already crossed H messages for the joiner and changed
                // the configuration, but the (H + 1)th, (H + 2)th... Kth messages show up
                // at monitors after they've already added the joiner. In this case, we simply
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
            }
            else {
                responseBuilder = responseBuilder.setStatusCode(JoinStatusCode.CONFIG_CHANGED);
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
    void processLinkUpdateMessage(final LinkUpdateMessageWire request) {
        Objects.requireNonNull(request);

        // TODO: move away from using two classes for the same message type
        final LinkUpdateMessage msg = new LinkUpdateMessage(request.getLinkSrc(), request.getLinkDst(),
                request.getLinkStatus(), request.getConfigurationId(),
                UUID.fromString(request.getUuid()), request.getRingNumber());

        final long currentConfigurationId = membershipView.getCurrentConfigurationId();
        if (currentConfigurationId != msg.getConfigurationId()) {
            LOG.trace("LinkUpdateMessage for configuration {} received during configuration {}",
                      msg.getConfigurationId(), currentConfigurationId);
            return;
        }

        // The invariant we want to maintain is that a node can only go into the
        // membership set once and leave it once.
        if (msg.getStatus().equals(LinkStatus.UP) && membershipView.isHostPresent(msg.getDst())) {
            LOG.trace("LinkUpdateMessage with status UP received for node {} already in configuration {} ",
                      msg.getDst(), currentConfigurationId);
            return;
        }
        if (msg.getStatus().equals(LinkStatus.DOWN) && !membershipView.isHostPresent(msg.getDst())) {
            LOG.trace("LinkUpdateMessage with status DOWN received for node {} already in configuration {} ",
                    msg.getDst(), currentConfigurationId);
            return;
        }

        final List<WatermarkBuffer.Node> proposal = proposedViewChange(msg);
        if (proposal.size() != 0) {
            // Initiate proposal
            if (logProposals) {
                logProposalList.add(proposal);
            }
            // Initiate consensus from here.

            // TODO: for now, we just apply the proposal directly.
            for (final WatermarkBuffer.Node node: proposal) {
                try {
                    membershipView.ringAdd(node.hostAndPort, node.uuid);
                } catch (final MembershipView.NodeAlreadyInRingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private List<WatermarkBuffer.Node> proposedViewChange(final LinkUpdateMessage msg) {
        return watermarkBuffer.aggregateForProposal(msg);
    }

    List<List<WatermarkBuffer.Node>> getProposalLog() {
        return Collections.unmodifiableList(logProposalList);
    }

    List<HostAndPort> getMembershipView() {
        return membershipView.viewRing(0);
    }
}