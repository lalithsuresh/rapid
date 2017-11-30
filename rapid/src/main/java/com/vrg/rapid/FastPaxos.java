package com.vrg.rapid;

import com.google.protobuf.TextFormat;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.ConsensusResponse;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
    private static final long BASE_DELAY = 1000;
    private final double jitterRate;
    private final Endpoint myAddr;
    private final long configurationId;
    private final long membershipSize;
    private final Consumer<List<Endpoint>> onDecidedWrapped;
    private final IBroadcaster broadcaster;
    private final Map<List<Endpoint>, AtomicInteger> votesPerProposal = new HashMap<>();
    private final Set<Endpoint> votesReceived = new HashSet<>(); // Should be a bitset
    private final Paxos paxos;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Object paxosLock = new Object();
    private final AtomicBoolean decided = new AtomicBoolean(false);
    @Nullable private ScheduledFuture<?> scheduledClassicRoundTask = null;

    FastPaxos(final Endpoint myAddr, final long configurationId, final int membershipSize,
              final IMessagingClient client, final IBroadcaster broadcaster,
              final ScheduledExecutorService scheduledExecutorService, final Consumer<List<Endpoint>> onDecide) {
        this.myAddr = myAddr;
        this.configurationId = configurationId;
        this.membershipSize = membershipSize;
        this.broadcaster = broadcaster;

        // The rate of a random expovariate variable, used to determine a jitter over a base delay to start classic
        // rounds. This determines how many classic rounds we want to start per second on average. Does not
        // affect correctness of the protocol, but having too many nodes starting rounds will increase messaging load,
        // especially for very large clusters.
        this.jitterRate = 1 / (double) membershipSize;
        this.scheduledExecutorService = scheduledExecutorService;
        this.onDecidedWrapped = hosts -> {
            assert !decided.get();
            decided.set(true);
            if (scheduledClassicRoundTask != null) {
                scheduledClassicRoundTask.cancel(true);
            }
            onDecide.accept(hosts);
        };
        this.paxos = new Paxos(myAddr, configurationId, membershipSize, client, broadcaster, onDecidedWrapped);
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
        final FastRoundPhase2bMessage consensusMessage = FastRoundPhase2bMessage.newBuilder()
                .setConfigurationId(configurationId)
                .addAllEndpoints(proposal)
                .setSender(myAddr)
                .build();
        final RapidRequest proposalMessage = Utils.toRapidRequest(consensusMessage);
        broadcaster.broadcast(proposalMessage);
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
        if (proposalMessage.getConfigurationId() != configurationId) {
            LOG.trace("Settings ID mismatch for proposal: current_config:{} proposal:{}", configurationId,
                      TextFormat.shortDebugString(proposalMessage));
            return;
        }

        if (votesReceived.contains(proposalMessage.getSender())) {
            return;
        }

        if (decided.get()) {
            return;
        }
        votesReceived.add(proposalMessage.getSender());
        final AtomicInteger proposalsReceived = votesPerProposal.computeIfAbsent(proposalMessage.getEndpointsList(),
                k -> new AtomicInteger(0));
        final int count = proposalsReceived.incrementAndGet();
        final int F = (int) Math.floor((membershipSize - 1) / 4.0); // Fast Paxos resiliency.
        if (votesReceived.size() >= membershipSize - F) {
            if (count >= membershipSize - F) {
                LOG.trace("Decided on a view change: {}", proposalMessage.getEndpointsList());
                // We have a successful proposal. Consume it.
                onDecidedWrapped.accept(proposalMessage.getEndpointsList());
            } else {
                // fallback protocol here
                LOG.trace("Fast round may not succeed for proposal: {}", proposalMessage.getEndpointsList());
            }
        }
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
        return jitter + BASE_DELAY;
    }
}
