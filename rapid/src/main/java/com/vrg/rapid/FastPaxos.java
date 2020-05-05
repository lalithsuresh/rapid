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

import com.google.protobuf.ByteString;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.ConsensusResponse;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.Proposal;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Single-decree consensus. We always start with a Fast round.
 */
class FastPaxos {
    private static final Logger LOG = LoggerFactory.getLogger(FastPaxos.class);
    static final long BASE_DELAY = 1000;
    private final double jitterRate;
    private final Endpoint myAddr;
    private final long configurationId;
    private final List<Endpoint> memberList;
    private final Consumer<List<Endpoint>> onDecidedWrapped;
    private final IBroadcaster broadcaster;
    private final Map<List<Endpoint>, AtomicInteger> votesPerProposal = new HashMap<>();
    private final Set<Endpoint> votesReceived = new HashSet<>(); // Should be a bitset
    private final Paxos paxos;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Object paxosLock = new Object();
    private final AtomicBoolean decided = new AtomicBoolean(false);
    @Nullable private ScheduledFuture<?> scheduledClassicRoundTask = null;
    private final ISettings settings;

    FastPaxos(final Endpoint myAddr, final long configurationId, final List<Endpoint> memberList,
              final IMessagingClient client, final IBroadcaster broadcaster,
              final ScheduledExecutorService scheduledExecutorService, final Consumer<List<Endpoint>> onDecide,
              final ISettings settings) {
        this.myAddr = myAddr;
        this.configurationId = configurationId;
        this.memberList = memberList;
        this.broadcaster = broadcaster;
        this.settings = settings;

        // The rate of a random expovariate variable, used to determine a jitter over a base delay to start classic
        // rounds. This determines how many classic rounds we want to start per second on average. Does not
        // affect correctness of the protocol, but having too many nodes starting rounds will increase messaging load,
        // especially for very large clusters.
        this.jitterRate = 1 / (double) this.memberList.size();
        this.scheduledExecutorService = scheduledExecutorService;
        this.onDecidedWrapped = hosts -> {
            assert !decided.get();
            decided.set(true);
            if (scheduledClassicRoundTask != null) {
                scheduledClassicRoundTask.cancel(true);
            }
            onDecide.accept(hosts);
        };
        this.paxos = new Paxos(myAddr, configurationId, this.memberList.size(), client, broadcaster, onDecidedWrapped);
    }

    /**
     * Propose a value for a fast round with a delay to trigger the recovery protocol.
     *
     * @param proposal the membership change proposal towards a configuration change.
     */
    void propose(final List<Endpoint> proposal, final long recoveryDelayInMs) {
        synchronized (paxosLock) {
            paxos.registerFastRoundVote(proposal);
        }
        final int myIndex = memberList.indexOf(myAddr);
        final BitSet votes = new BitSet(memberList.size());
        votes.set(myIndex);
        final Proposal proposalAndVotes = Proposal.newBuilder()
                .setConfigurationId(configurationId)
                .addAllEndpoints(proposal)
                .setVotes(ByteString.copyFrom(votes.toByteArray()))
                .build();
        final FastRoundPhase2bMessage consensusMessage = FastRoundPhase2bMessage.newBuilder()
                .addProposals(proposalAndVotes)
                .build();
        final RapidRequest proposalMessage = Utils.toRapidRequest(consensusMessage);
        broadcaster.broadcast(proposalMessage, configurationId);
        LOG.trace("Scheduling classic round with delay: {}", recoveryDelayInMs);
        scheduledClassicRoundTask = scheduledExecutorService.schedule(this::startClassicPaxosRound, recoveryDelayInMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Propose a value for a fast round.
     *
     * @param proposal the membership change proposal towards a configuration change.
     */
    void propose(final List<Endpoint> proposal) {
        propose(proposal, getRandomDelayMs());
    }


    /**
     * Invoked by the membership service when it receives a proposal for a fast round.
     *
     * @param proposalMessage the membership change proposal towards a configuration change.
     */
    private void handleFastRoundProposal(final FastRoundPhase2bMessage proposalMessage) {
        if (decided.get()) {
            return;
        }

        proposalMessage
                .getProposalsList()
                .stream()
                .filter(proposal -> {
                    final boolean hasSameConfigurationId = proposal.getConfigurationId() == configurationId;
                    if (!hasSameConfigurationId) {
                        LOG.warn("Settings ID mismatch for proposal: current_config:{} proposal_config:{}" +
                                        "proposal of size {}", configurationId, proposal.getConfigurationId(),
                                        proposal.getEndpointsCount());
                    }
                    return hasSameConfigurationId;
                }).forEach(proposal -> {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received proposal for {} nodes", proposal.getEndpointsCount());
            }
            // decompress all the votes contained in the proposal
            // the index in the bitset corresponds to the index of an endpoint in ring 0
            final AtomicInteger proposalsReceived = votesPerProposal.computeIfAbsent(
                    proposal.getEndpointsList(),
                    k -> new AtomicInteger(0));
            final BitSet votes = BitSet.valueOf(proposal.getVotes().toByteArray());
            for (int i = 0; i < votes.length(); i++) {
                if (votes.get(i)) {
                    final Endpoint voter = memberList.get(i);
                    if (!votesReceived.contains(voter)) {
                        votesReceived.add(voter);
                        proposalsReceived.incrementAndGet();
                    }
                }
            }

            // now check if we have reached agreement
            final int count = votesPerProposal.get(proposal.getEndpointsList()).get();
            LOG.trace("Currently {} votes for the proposal with {} nodes, {} votes in total",
                    count, proposal.getEndpointsCount(), votesReceived.size());
            final int F = (int) Math.floor((memberList.size() - 1) / 4.0); // Fast Paxos resiliency.
            if (votesReceived.size() >= memberList.size() - F) {
                if (count >= memberList.size() - F) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Decided on a view change: {}", proposal.getEndpointsList());
                    }
                    // We have a successful proposal. Consume it.
                    onDecidedWrapped.accept(proposal.getEndpointsList());
                } else {
                    // fallback protocol here
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Fast round may not succeed for proposal: {}", proposal.getEndpointsList());
                    }
                }
            }
        });

    }

    /**
     * Invoked by the membership service when it receives a consensus proposal.
     *
     * @param request the membership change proposal towards a configuration change.
     */
    RapidResponse handleMessages(final RapidRequest request) {
        switch (request.getContentCase()) {
            case FASTROUNDPHASE2BMESSAGE:
                handleFastRoundProposal(request.getFastRoundPhase2BMessage());
                break;
            case PHASE1AMESSAGE:
                paxos.handlePhase1aMessage(request.getPhase1AMessage());
                break;
            case PHASE1BMESSAGE:
                paxos.handlePhase1bMessage(request.getPhase1BMessage());
                break;
            case PHASE2AMESSAGE:
                paxos.handlePhase2aMessage(request.getPhase2AMessage());
                break;
            case PHASE2BMESSAGE:
                paxos.handlePhase2bMessage(request.getPhase2BMessage());
                break;
            default:
                throw new IllegalArgumentException("Unexpected message case: " + request.getContentCase());
        }
        return Utils.toRapidResponse(ConsensusResponse.getDefaultInstance());
    }

    /**
     * Trigger Paxos phase1a.
     */
    void startClassicPaxosRound() {
        if (!decided.get()) {
            synchronized (paxosLock) {
                paxos.startPhase1a(2);
            }
        }
    }

    /**
     * Random expovariate variable plus a base delay.
     */
    private long getRandomDelayMs() {
        final long jitter = (long) (-1000 * Math.log(1 - ThreadLocalRandom.current().nextDouble()) / jitterRate);
        return jitter + settings.getConsensusFallbackTimeoutBaseDelayInMs();
    }

    interface ISettings {
        long getConsensusFallbackTimeoutBaseDelayInMs();
    }
}
