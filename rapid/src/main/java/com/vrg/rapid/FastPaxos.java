package com.vrg.rapid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.protobuf.TextFormat;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.ConsensusResponse;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Single-decree consensus. We always start with a Fast round.
 */
class FastPaxos {
    private static final Logger LOG = LoggerFactory.getLogger(FastPaxos.class);
    private final HostAndPort myAddr;
    private final long configurationId;
    private final long membershipSize;
    private final Consumer<List<HostAndPort>> onDecide;
    private final IBroadcaster broadcaster;
    private final Map<List<String>, AtomicInteger> votesPerProposal = new HashMap<>();
    private final Set<String> votesReceived = new HashSet<>(); // Should be a bitset
    private final Paxos paxos;
    private boolean decided = false;

    FastPaxos(final HostAndPort myAddr, final long configurationId, final int membershipSize,
              final IMessagingClient client, final IBroadcaster broadcaster,
              final Consumer<List<HostAndPort>> onDecide) {
        this.myAddr = myAddr;
        this.configurationId = configurationId;
        this.onDecide = onDecide;
        this.membershipSize = membershipSize;
        this.broadcaster = broadcaster;
        this.paxos = new Paxos(myAddr, configurationId, membershipSize, broadcaster, client, onDecide);
    }

    /**
     * Propose a value for a fast round.
     *
     * @param proposal the membership change proposal towards a configuration change.
     */
    void propose(final List<HostAndPort> proposal) {
        paxos.registerFastRoundVote(proposal);
        final FastRoundPhase2bMessage consensusMessage = FastRoundPhase2bMessage.newBuilder()
                .setConfigurationId(configurationId)
                .addAllHosts(proposal.stream()
                        .map(HostAndPort::toString)
                        .sorted()
                        .collect(Collectors.toList()))
                .setSender(myAddr.toString())
                .build();
        final RapidRequest proposalMessage = Utils.toRapidRequest(consensusMessage);
        broadcaster.broadcast(proposalMessage);
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

        if (decided) {
            return;
        }
        votesReceived.add(proposalMessage.getSender());
        final AtomicInteger proposalsReceived = votesPerProposal.computeIfAbsent(proposalMessage.getHostsList(),
                k -> new AtomicInteger(0));
        final int count = proposalsReceived.incrementAndGet();
        final int F = (int) Math.floor((membershipSize - 1) / 4.0); // Fast Paxos resiliency.
        if (votesReceived.size() >= membershipSize - F) {
            if (count >= membershipSize - F) {
                LOG.trace("{} has decided on a view change: {}", myAddr, proposalMessage.getHostsList());
                // We have a successful proposal. Consume it.
                onDecide.accept(proposalMessage.getHostsList()
                        .stream()
                        .map(HostAndPort::fromString)
                        .collect(Collectors.toList()));
                decided = true;
            } else {
                // fallback protocol here
                LOG.trace("{} fast round may not succeed for {}", myAddr, proposalMessage.getHostsList());
                startClassicPaxosRound();
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
    @VisibleForTesting
    void startClassicPaxosRound() {
        paxos.startPhase1a(1);
    }
}
