package com.vrg.rapid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.TextFormat;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.Endpoint;
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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 *  Single-decree consensus. Implements classic Paxos with the modified rule for the coordinator to pick values as per
 *  the Fast Paxos paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
 *
 *  The code below assumes that the first round in a consensus instance (done per configuration change) is the
 *  only round that is a fast round. A round is identified by a tuple (rnd-number, nodeId), where nodeId is a unique
 *  identifier per node that initiates phase1.
 */
@NotThreadSafe
class Paxos {
    private static final Logger LOG = LoggerFactory.getLogger(Paxos.class);

    private final IBroadcaster broadcaster;
    private final IMessagingClient client;
    private final long configurationId;
    private final Endpoint myAddr;
    private final int N;

    private Rank rnd;
    private Rank vrnd;
    private List<Endpoint> vval;
    private final List<Phase1bMessage> phase1bMessages = new ArrayList<>();
    private final List<Phase2bMessage> acceptResponses = new ArrayList<>();

    private Rank crnd;
    private List<Endpoint> cval;

    private final Consumer<List<Endpoint>> onDecide;
    private boolean decided = false;

    public Paxos(final Endpoint myAddr, final long configurationId, final int N, final IMessagingClient client,
                 final IBroadcaster broadcaster, final Consumer<List<Endpoint>> onDecide) {
        this.myAddr = myAddr;
        this.configurationId = configurationId;
        this.N = N;
        this.broadcaster = broadcaster;

        this.crnd = Rank.newBuilder().setRound(0).setNodeIndex(0).build();
        this.rnd = Rank.newBuilder().setRound(0).setNodeIndex(0).build();
        this.vrnd = Rank.newBuilder().setRound(0).setNodeIndex(0).build();
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
        crnd = crnd.toBuilder().setRound(round).setNodeIndex(myAddr.hashCode()).build();
        LOG.trace("Prepare called for round {}", Utils.loggable(crnd));
        final Phase1aMessage prepare = Phase1aMessage.newBuilder()
                                       .setConfigurationId(configurationId)
                                       .setSender(myAddr)
                                       .setRank(crnd).build();
        final RapidRequest request = Utils.toRapidRequest(prepare);
        LOG.trace("Broadcasting startPhase1a message: {}", Utils.loggable(request));
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
            LOG.trace("Rejecting prepareMessage from lower rank: ({}) ({})", Utils.loggable(rnd),
                    Utils.loggable(phase1aMessage));
            return;
        }
        LOG.trace("Sending back vval:{} vrnd:{}", vval, TextFormat.shortDebugString(vrnd));
        final Phase1bMessage phase1bMessage = Phase1bMessage.newBuilder()
                                      .setConfigurationId(configurationId)
                                      .setRnd(rnd)
                                      .setSender(myAddr)
                                      .setVrnd(vrnd)
                                      .addAllVval(vval)
                                      .build();
        final RapidRequest request = Utils.toRapidRequest(phase1bMessage);
        final ListenableFuture<RapidResponse> rapidResponseListenableFuture =
                client.sendMessage(phase1aMessage.getSender(), request);
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

        LOG.trace("Handling PrepareResponse: {}", Utils.loggable(phase1bMessage));

        phase1bMessages.add(phase1bMessage);

        if (phase1bMessages.size() > (N / 2)) {
            // selectProposalUsingCoordinator rule may execute multiple times with each additional phase1bMessage
            // being received, but we can enter the following if statement only once when a valid cval is identified.
            final List<Endpoint> chosenProposal = selectProposalUsingCoordinatorRule(phase1bMessages);
            if (crnd.equals(phase1bMessage.getRnd()) && cval.size() == 0 && chosenProposal.size() > 0) {
                LOG.trace("Proposing: {}", Utils.loggable(chosenProposal));
                cval = chosenProposal;
                final Phase2aMessage phase2aMessage = Phase2aMessage.newBuilder()
                                                   .setSender(myAddr)
                                                   .setConfigurationId(configurationId)
                                                   .setRnd(crnd)
                                                   .addAllVval(chosenProposal)
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

        LOG.trace("At acceptor received phase2aMessage: {}", Utils.loggable(phase2aMessage));
        if (compareRanks(rnd, phase2aMessage.getRnd()) <= 0 && !vrnd.equals(phase2aMessage.getRnd())) {
            vrnd = phase2aMessage.getRnd();
            vval = phase2aMessage.getVvalList();
            LOG.trace("Accepted value in vrnd: {}, vval: {}", Utils.loggable(vrnd), Utils.loggable(vval));

            final Phase2bMessage response = Phase2bMessage.newBuilder()
                                                          .setConfigurationId(configurationId)
                                                          .setRnd(phase2aMessage.getRnd())
                                                          .addAllEndpoints(vval)
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
        LOG.trace("Received phase2bMessage: {}", Utils.loggable(phase2bMessage));
        acceptResponses.add(phase2bMessage);
        if (acceptResponses.size() > (N / 2) && !decided) {
            final List<Endpoint> decision = phase2bMessage.getEndpointsList();
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
    void registerFastRoundVote(final List<Endpoint> vote) {
        // Do not participate in our only fast round if we are already participating in a classic round.
        if (rnd.getRound() > 1) {
            return;
        }
        // This is the 1st round in the consensus instance, is always a fast round, and is always the *only* fast round.
        // If this round does not succeed and we fallback to a classic round, we start with round number 2
        // and each node sets its node-index as the hash of its hostname. Doing so ensures that all classic
        // rounds initiated by any host is higher than the fast round, and there is an ordering between rounds
        // initiated by different endpoints.
        rnd = rnd.toBuilder().setRound(1).setNodeIndex(1).build();
        vrnd = rnd;
        vval = vote;
        LOG.trace("Voted in fast round for proposal: {}", Utils.loggable(vote));
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
    List<Endpoint> selectProposalUsingCoordinatorRule(final List<Phase1bMessage> phase1bMessages) {
        final Rank maxVrndSoFar = phase1bMessages.stream().map(Phase1bMessage::getVrnd)
                                  .max(Paxos::compareRanks)
                                  .orElseThrow(() -> new IllegalArgumentException("phase1bMessages was empty"));

        // Let k be the largest value of vr(a) for all a in Q.
        // V (collectedVvals) be the set of all vv(a) for all a in Q s.t vr(a) == k
        final List<List<Endpoint>> collectedVvals = phase1bMessages.stream()
                .filter(r -> r.getVrnd().equals(maxVrndSoFar))
                .filter(r -> r.getVvalCount() > 0)
                .map(Phase1bMessage::getVvalList)
                .collect(Collectors.toList());
        List<Endpoint> chosenProposal = null;

        // If V has a single element, then choose v.
        if (collectedVvals.size() == 1) {
            chosenProposal = collectedVvals.iterator().next();
        }
        // if i-quorum Q of acceptors respond, and there is a k-quorum R such that vrnd = k and vval = v,
        // for all a in intersection(R, Q) -> then choose "v". When choosing E = N/4 and F = N/2, then
        // R intersection Q is N/4 -- meaning if there are more than N/4 identical votes.
        else if (collectedVvals.size() > 1) {
            // multiple values were proposed, so we need to check if there is a majority with the same value.
            final Map<List<Endpoint>, Integer> counters = new HashMap<>();
            for (final List<Endpoint> value: collectedVvals) {
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
        // At this point, no value has been selected yet and it is safe for the coordinator to pick any proposed value.
        // If none of the 'vvals' contain valid values (are all empty lists), then this method returns an empty
        // list. This can happen because a quorum of acceptors that did not vote in prior rounds may have responded
        // to the coordinator first. This is safe to do here for two reasons:
        //      1) The coordinator will only proceed with phase 2 if it has a valid vote.
        //      2) It is likely that the coordinator (itself being an acceptor) is the only one with a valid vval,
        //         and has not heard a Phase1bMessage from itself yet. Once that arrives, phase1b will be triggered
        //         again.
        //
        // XXX: one option is to propose a new value of our own that is the union of all proposed values so far.
        if (chosenProposal == null) {
            chosenProposal = phase1bMessages.stream()
                                            .filter(r -> r.getVvalCount() > 0)
                                            .map(Phase1bMessage::getVvalList)
                                            .findFirst().orElse(Collections.emptyList());
            LOG.trace("Proposing new value -- chosen:{}, list:{}, vrnd:{}", Utils.loggable(chosenProposal),
                      collectedVvals, Utils.loggable(maxVrndSoFar));
        }
        return chosenProposal;
    }

    /**
     * Primary ordering is by round number, and secondary ordering by the ID of the node that initiated the round.
     */
    private static int compareRanks(final Rank left, final Rank right) {
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