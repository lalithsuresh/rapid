package com.vrg.rapid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.TextFormat;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.Phase1aMessage;
import com.vrg.rapid.pb.Phase1bMessage;
import com.vrg.rapid.pb.Phase2aMessage;
import com.vrg.rapid.pb.Phase2bMessage;
import com.vrg.rapid.pb.Rank;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 *  Single-decree consensus. Implements classic Paxos with the modified rule for the coordinator to pick values as per
 *  the Fast Paxos paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
 *
 *  The code below assumes that the first round in a consensus instance (done per configuratin change) is the
 *  only round that is a fast round. A round is identified by a tuple (rnd-number, nodeId), where nodeId is a unique
 *  identifier per node that initiates phase1.
 */
@NotThreadSafe
public class Paxos {
    private static final Logger LOG = LoggerFactory.getLogger(Paxos.class);

    private final IBroadcaster broadcaster;
    private final IMessagingClient client;
    private final long configurationId;
    private final HostAndPort myAddr;
    private final int N;

    private Rank rnd;
    private Rank vrnd;
    private List<HostAndPort> vval;
    private List<Phase1bMessage> phase1bMessages = new ArrayList<>();
    private List<Phase2bMessage> acceptResponses = new ArrayList<>();

    private Rank crnd;
    private List<HostAndPort> cval;
    private Rank maxVrndSoFar = Rank.newBuilder().setRound(Integer.MIN_VALUE).setNodeIndex(Integer.MIN_VALUE).build();

    private final Consumer<List<HostAndPort>> onDecide;
    private boolean decided = false;

    public Paxos(final HostAndPort myAddr, final long configurationId, final int N, final IMessagingClient client,
                 final IBroadcaster broadcaster, final Consumer<List<HostAndPort>> onDecide) {
        this.myAddr = myAddr;
        this.configurationId = configurationId;
        this.N = N;
        this.broadcaster = broadcaster;

        this.crnd = Rank.newBuilder().setRound(0).setNodeIndex(myAddr.hashCode()).build();
        this.rnd = crnd.toBuilder().build();
        this.vrnd = crnd.toBuilder().build();
        this.vval = Collections.emptyList();
        this.cval = Collections.emptyList();
        this.client = client;
        this.onDecide = onDecide;
    }

    /**
     * At coordinator, start a classic round. We ensure that even if round numbers are not unique, the
     * "rank" = (round, nodeId) is unique by using unique node IDs.
     *
     * @param round The round number to initiate.
     */
    void startPhase1a(final int round) {
        if (crnd.getRound() > round) {
            return;
        }
        crnd = crnd.toBuilder().setRound(round).build();
        LOG.trace("Prepare called for round {}", TextFormat.shortDebugString(crnd));
        final Phase1aMessage prepare = Phase1aMessage.newBuilder()
                                       .setConfigurationId(configurationId)
                                       .setSender(myAddr.toString())
                                       .setRank(crnd).build();
        final RapidRequest request = Utils.toRapidRequest(prepare);
        LOG.trace("Broadcasting startPhase1a message: {}", TextFormat.shortDebugString(request));
        broadcaster.broadcast(request);
    }

    /**
     * At acceptor, handle a phase1a message from a coordinator.
     *
     * @param phase1aMessage phase1a message from a coordinator.
     */
    void handlePhase1aMessage(final Phase1aMessage phase1aMessage) {
        if (phase1aMessage.getConfigurationId() != configurationId) {
            return;
        }

        if (compareRanks(rnd, phase1aMessage.getRank()) < 0) {
            rnd = phase1aMessage.getRank();
        }
        else {
            LOG.trace("Rejecting prepareMessage from lower rank: ({}) ({})", TextFormat.shortDebugString(rnd),
                    TextFormat.shortDebugString(phase1aMessage));
            return;
        }
        LOG.trace("Sending back vval {} ", vval);
        final Phase1bMessage phase1bMessage = Phase1bMessage.newBuilder()
                                      .setConfigurationId(configurationId)
                                      .setRnd(rnd)
                                      .setSender(myAddr.toString())
                                      .setVrnd(vrnd)
                                      .addAllVval(vval.stream().map(HostAndPort::toString).sorted()
                                                  .collect(Collectors.toList()))
                                      .build();
        final RapidRequest request = Utils.toRapidRequest(phase1bMessage);
        final ListenableFuture<RapidResponse> rapidResponseListenableFuture =
                client.sendMessage(HostAndPort.fromString(phase1aMessage.getSender()), request);
        Futures.addCallback(rapidResponseListenableFuture, new ResponseCallback());
    }

    /**
     * At coordinator, collect phase1b messages from acceptors to learn whether they have already voted for
     * any values, and if a value might have been chosen.
     *
     * @param phase1bMessage startPhase1a response messages from acceptors.
     */
    void handlePhase1bMessage(final Phase1bMessage phase1bMessage) {
        if (phase1bMessage.getConfigurationId() != configurationId) {
            return;
        }

        // Only handle responses from crnd == i
        if (compareRanks(crnd, phase1bMessage.getRnd()) != 0) {
            return;
        }

        LOG.trace("Handling PrepareResponse: {}", TextFormat.shortDebugString(phase1bMessage));

        phase1bMessages.add(phase1bMessage);
        maxVrndSoFar = compareRanks(maxVrndSoFar, phase1bMessage.getVrnd()) >= 0 ?
                        maxVrndSoFar : phase1bMessage.getVrnd();
        if (phase1bMessages.size() > (N / 2)) {
            final List<HostAndPort> chosenProposal = selectProposalUsingCoordinatorRule(phase1bMessages);
            if (crnd.equals(phase1bMessage.getRnd()) && cval.size() == 0 && chosenProposal.size() > 0) {
                LOG.trace("Proposing: {}", chosenProposal);
                cval = chosenProposal;
                final Phase2aMessage phase2aMessage = Phase2aMessage.newBuilder()
                                                   .setSender(myAddr.toString())
                                                   .setConfigurationId(configurationId)
                                                   .setRnd(crnd)
                                                   .addAllVval(chosenProposal.stream()
                                                         .map(HostAndPort::toString)
                                                         .sorted()
                                                         .collect(Collectors.toList()))
                                                   .build();
                final RapidRequest request = Utils.toRapidRequest(phase2aMessage);
                broadcaster.broadcast(request);
            }
        }
    }

    /**
     * At acceptor, handle an accept message from a coordinator.
     *
     * @param phase2aMessage accept message from coordinator
     */
    void handlePhase2aMessage(final Phase2aMessage phase2aMessage) {
        if (phase2aMessage.getConfigurationId() != configurationId) {
            return;
        }

        LOG.trace("At acceptor received phase2aMessage: {}", TextFormat.shortDebugString(phase2aMessage));
        if (compareRanks(rnd, phase2aMessage.getRnd()) <= 0 && !vrnd.equals(phase2aMessage.getRnd())) {
            vrnd = phase2aMessage.getRnd();
            vval = phase2aMessage.getVvalList().stream().map(HostAndPort::fromString).collect(Collectors.toList());
            LOG.trace("Accepted value in vrnd: {}, vval: {}", TextFormat.shortDebugString(vrnd), vval);

            final Phase2bMessage response = Phase2bMessage.newBuilder()
                                                          .setConfigurationId(configurationId)
                                                          .setRnd(phase2aMessage.getRnd())
                                                          .addAllHosts(vval.stream().map(HostAndPort::toString)
                                                                        .sorted()
                                                                        .collect(Collectors.toList()))
                                                          .build();
            final RapidRequest request = Utils.toRapidRequest(response);
            broadcaster.broadcast(request);
        }
    }

    /**
     * At acceptor, learn about another acceptor's vote (phase2b messages).
     *
     * @param phase2bMessage acceptor's vote
     */
    void handlePhase2bMessage(final Phase2bMessage phase2bMessage) {
        if (phase2bMessage.getConfigurationId() != configurationId) {
            return;
        }
        LOG.trace("Received phase2bMessage: {}", TextFormat.shortDebugString(phase2bMessage));
        acceptResponses.add(phase2bMessage);
        if (acceptResponses.size() > (N / 2) && !decided) {
            final List<HostAndPort> decision = phase2bMessage.getHostsList().stream().map(HostAndPort::fromString)
                    .collect(Collectors.toList());
            LOG.trace("Decided on: {}", decision);
            onDecide.accept(decision);
            decided = true;
        }
    }

    /**
     * This is how we're notified that a fast round is initiated. Invoked by a FastPaxos instance. This
     * represents the logic at an acceptor receiving a phase2a message directly.
     *
     * @param vote the vote for the fast round
     */
    void registerFastRoundVote(final List<HostAndPort> vote) {
        // Do not participate in our only fast round if we are already participating in a classic round.
        if (rnd.getRound() > 0) {
            return;
        }
        // This is the 0th round in the consensus instance, is always a fast round, and is always the *only* fast round.
        // If this round does not succeed and we fallback to a classic round, we start with round number 1
        // and each node sets its node-index as the hash of its hostname. Doing so ensures that all classic
        // rounds initiated by any host is higher than the fast round, and there is an ordering between rounds
        // initiated by different hosts.
        rnd = rnd.toBuilder().setRound(0).setNodeIndex(0).build();
        vrnd = rnd;
        vval = vote;
        LOG.trace("{} voted in fast round for proposal: {}", myAddr, vote);
    }

    /**
     * The rule with which a coordinator picks a value to propose based on the received phase1b messages.
     * This corresponds to the logic in Figure 2 of the Fast Paxos paper:
     * https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
     *
     * @param phase1bMessages A list of phase1b messages from acceptors.
     * @return a proposal to apply
     */
    @VisibleForTesting
    List<HostAndPort> selectProposalUsingCoordinatorRule(final List<Phase1bMessage> phase1bMessages) {
        // Let k be the largest value of vr(a) for all a in Q.
        // V (collectedVvals) be the set of all vv(a) for all a in Q s.t vr(a) == k
        final List<List<HostAndPort>> collectedVvals = phase1bMessages.stream()
                .filter(r -> r.getVrnd().equals(maxVrndSoFar))
                .filter(r -> r.getVvalCount() > 0)
                .map(Phase1bMessage::getVvalList)
                .map(v -> v.stream().map(HostAndPort::fromString).collect(Collectors.toList()))
                .collect(Collectors.toList());
        List<HostAndPort> chosenProposal = null;
        // If V has a single element, then choose v.
        if (collectedVvals.size() == 1) {
            chosenProposal = collectedVvals.iterator().next();
        }
        // if i-quorum Q of acceptors respond, and there is a k-quorum R such that vrnd = k and vval = v,
        // for all a in intersection(R, Q) -> then choose "v". When choosing E = N/4 and F = N/2, then
        // R intersection Q is N/4 -- meaning if there are more than N/4 identical votes.
        else if (collectedVvals.size() > 1) {
            // multiple values were proposed, so we need to check if there is a majority with the same value.
            final Map<List<HostAndPort>, Integer> counters = new HashMap<>();
            for (final List<HostAndPort> value: collectedVvals) {
                if (!counters.containsKey(value)) {
                    counters.put(value, 0);
                }
                final int count = counters.get(value);
                if (count + 1 > (N / 4)) {
                    chosenProposal = value;
                    break;
                }
                else {
                    counters.put(value, count + 1);
                }
            }
        }
        // At this point, no value has been selected yet and it is safe for the coordinator to propose a value of its
        // own which is the union of all existing values.
        if (chosenProposal == null) {
            final Set<HostAndPort> proposal = new HashSet<>();
            collectedVvals.forEach(proposal::addAll);
            chosenProposal = new ArrayList<>(proposal);
            LOG.trace("There were multiple values in round k -- chosen:{}, list:{}, vrnd:{}", chosenProposal,
                    collectedVvals, TextFormat.shortDebugString(maxVrndSoFar));
        }
        return chosenProposal;
    }

    /**
     * Primary ordering is by round number, and secondary ordering by the ID of the node that initiated the round.
     */
    private int compareRanks(final Rank left, final Rank right) {
        final int compRound = Integer.compare(left.getRound(), right.getRound());
        if (compRound == 0) {
            return Integer.compare(left.getNodeIndex(), right.getNodeIndex());
        }
        return compRound;
    }

    private static class ResponseCallback implements FutureCallback<RapidResponse> {
        @Override
        public void onSuccess(@Nullable final RapidResponse response) {
            LOG.trace("Successfully sent request");
        }

        @Override
        public void onFailure(final Throwable throwable) {
            LOG.error("Received exception: " + throwable);
        }
    }
}