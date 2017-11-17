package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.RapidRequest;
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

    FastPaxos(final HostAndPort myAddr, final long configurationId, final long membershipSize,
              final IBroadcaster broadcaster, final Consumer<List<HostAndPort>> onDecide) {
        this.myAddr = myAddr;
        this.configurationId = configurationId;
        this.onDecide = onDecide;
        this.membershipSize = membershipSize;
        this.broadcaster = broadcaster;
    }

    /**
     * Propose a value for a fast round.
     *
     * @param proposal the membership change proposal towards a configuration change.
     */
    void propose(final List<HostAndPort> proposal) {
        final RapidRequest proposalMessage = RapidRequest.newBuilder()
                                                     .setConsensusProposal(ConsensusProposal.newBuilder()
                                                                            .setConfigurationId(configurationId)
                                                                            .addAllHosts(proposal
                                                                                         .stream()
                                                                                         .map(HostAndPort::toString)
                                                                                         .sorted()
                                                                                         .collect(Collectors.toList()))
                                                                            .setSender(myAddr.toString())
                                                                            .build())
                                                     .build();
        broadcaster.broadcast(proposalMessage);
    }

    /**
     * Invoked by the membership service when it receives a consensus proposal.
     *
     * @param proposalMessage the membership change proposal towards a configuration change.
     */
    void handleFastRoundProposal(final ConsensusProposal proposalMessage) {
        if (proposalMessage.getConfigurationId() != configurationId) {
            LOG.trace("Settings ID mismatch for proposal: current_config:{} proposal:{}", configurationId,
                      proposalMessage);
            return;
        }

        if (votesReceived.contains(proposalMessage.getSender())) {
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
            } else {
                // fallback protocol here
                LOG.trace("{} fast round may not succeed for {}", myAddr, proposalMessage.getHostsList());
            }
        }
    }
}
