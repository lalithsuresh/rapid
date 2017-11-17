package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.ConsensusResponse;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for Paxos protocol
 */
@RunWith(JUnitParamsRunner.class)
public class PaxosTests {
    private Set<RapidRequest.ContentCase> messageTypeToDrop = new HashSet<>();

    @Before
    public void beforeTest() {
        messageTypeToDrop.clear();
    }

    /**
     * Test multiple nodes issuing different proposals in parallel
     */
    @Test
    @Parameters(method = "nValues")
    @TestCaseName("{method}[N={0}]")
    public void testRecoveryForSinglePropose(final int N) throws InterruptedException {
        final int numNodes = N;
        final ExecutorService executorService = Executors.newFixedThreadPool(numNodes);
        final LinkedBlockingDeque<List<HostAndPort>> decisions = new LinkedBlockingDeque<>();
        final Consumer<List<HostAndPort>> onDecide = decisions::add;
        final Map<HostAndPort, FastPaxos> instances = createNFastPaxosInstances(numNodes, onDecide);
        final Map.Entry<HostAndPort, FastPaxos> any = instances.entrySet().stream().findAny().get();
        final List<HostAndPort> proposal = Collections.singletonList(HostAndPort.fromString("172.14.12.3"));
        executorService.execute(() -> {
            any.getValue().propose(proposal);
            any.getValue().startClassicPaxosRound();
        });
        waitAndVerifyAgreement(numNodes, 20, 50, decisions);
        assertAll(proposal, decisions);
    }

    /**
     * Test multiple nodes issuing different proposals in parallel
     */
    @Test
    @Parameters(method = "nValues")
    @TestCaseName("{method}[N={0}]")
    public void testRecoveryFromFastRoundWithDifferentProposals(final int N) throws InterruptedException {
        final int numNodes = N;
        final ExecutorService executorService = Executors.newFixedThreadPool(numNodes);
        final LinkedBlockingDeque<List<HostAndPort>> decisions = new LinkedBlockingDeque<>();
        final Consumer<List<HostAndPort>> onDecide = decisions::add;
        final Map<HostAndPort, FastPaxos> instances = createNFastPaxosInstances(numNodes, onDecide);
        instances.forEach((host, fp) -> executorService.execute(() -> fp.propose(Collections.singletonList(host))));
        waitAndVerifyAgreement(numNodes, 20, 50, decisions);
        for (final List<HostAndPort> decision: decisions) {
            assertTrue(decision.size() > 1);
            assertTrue(instances.containsKey(decision.get(0))); // proposed values are host names
        }
    }

    /**
     * We mimic a scenario where a successful fast round happened but we didn't learn the decision
     * because messages were lost. A subsequent slow round should learn the result of the fast round.
     */
    @Test
    @Parameters(method = "nValues")
    @TestCaseName("{method}[N={0}]")
    public void testClassicRoundAfterSuccessfulFastRound(final int N) throws InterruptedException {
        final int numNodes = N;
        final ExecutorService executorService = Executors.newFixedThreadPool(numNodes);
        final LinkedBlockingDeque<List<HostAndPort>> decisions = new LinkedBlockingDeque<>();
        final Consumer<List<HostAndPort>> onDecide = decisions::add;
        final Map<HostAndPort, FastPaxos> instances = createNFastPaxosInstances(numNodes, onDecide);
        final List<HostAndPort> proposal = Collections.singletonList(HostAndPort.fromHost("127.0.0.1"));
        messageTypeToDrop.add(RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE);
        instances.forEach((host, fp) -> executorService.execute(() -> fp.propose(proposal)));
        waitAndVerifyAgreement(0, 20, 50, decisions);
        instances.forEach((host, fp) -> executorService.execute(fp::startClassicPaxosRound));
        waitAndVerifyAgreement(numNodes, 20, 50, decisions);
    }

    public static Iterable<Object[]> nValues() {
        // Format: (N, proposal-1, proposal-2, votes for proposal-2 (p2votes), expected value to be chosen)
        // proposal-1 gets all the remaining votes (N - p2votes).
        final List<Object[]> numNodesParams = Arrays.asList(new Object[][] {
                {5}, {6}, {10}, {11}, {20}
        });
        final ArrayList<Object[]> params = new ArrayList<>();
        params.addAll(numNodesParams);
        return params;
    }

    /**
     * We mimic a scenario where a successful fast round happened with a mix of messages but the acceptors did not
     * learn the decision because messages were lost. A subsequent slow round should learn the result of the fast round.
     */
    @Test
    @Parameters(method = "testClassicRoundAfterSuccessfulFastRoundMixedValues")
    @TestCaseName("{method}[N={0},p1={1},p2={2},p2Votes={3},decisionChoices={4}]")
    public void testClassicRoundAfterSuccessfulFastRoundMixedValues(final int N, final List<HostAndPort> p1,
                                                               final List<HostAndPort> p2, final int p2Votes,
                                                               final List<HostAndPort> decisionChoices)
            throws InterruptedException {
        final int numNodes = N;
        final ExecutorService executorService = Executors.newFixedThreadPool(numNodes);
        final LinkedBlockingDeque<List<HostAndPort>> decisions = new LinkedBlockingDeque<>();
        final Consumer<List<HostAndPort>> onDecide = decisions::add;
        final Map<HostAndPort, FastPaxos> instances = createNFastPaxosInstances(numNodes, onDecide);
        messageTypeToDrop.add(RapidRequest.ContentCase.FASTROUNDPHASE2BMESSAGE);
        int nodeIndex = 0;
        for (final Map.Entry<HostAndPort, FastPaxos> entry: instances.entrySet()) {
            if (nodeIndex < numNodes - p2Votes) {
                executorService.execute(() -> entry.getValue().propose(p1));
            }
            else {
                executorService.execute(() -> entry.getValue().propose(p2));
            }
            nodeIndex++;
        }
        waitAndVerifyAgreement(0, 20, 50, decisions);
        instances.forEach((host, fp) -> executorService.execute(fp::startClassicPaxosRound));
        waitAndVerifyAgreement(numNodes, 20, 50, decisions);
        if (decisionChoices.size() == 1) {
            assertAll(decisionChoices, decisions);
        }
        else {
            // Any of the proposed values would be correct to decide on
            assertTrue(decisions.getFirst().size() == 1);
            assertTrue(decisionChoices.contains(decisions.getFirst().get(0)));
            assertAll(decisions.getFirst(), decisions);
        }
    }

    public static Iterable<Object[]> testClassicRoundAfterSuccessfulFastRoundMixedValues() {
        final List<HostAndPort> p1 = Collections.singletonList(HostAndPort.fromHost("127.0.0.2"));
        final List<HostAndPort> p2 = Collections.singletonList(HostAndPort.fromHost("127.0.0.1"));
        final List<HostAndPort> p1p2 = new ArrayList<>();
        p1p2.addAll(p1);
        p1p2.addAll(p2);
        // Format: (N, proposal-1, proposal-2, votes for proposal-2 (p2votes), expected value to be chosen)
        // proposal-1 gets all the remaining votes (N - p2votes).
        final List<Object[]> oneConflictArray = Arrays.asList(new Object[][] {
                {6, p1, p2, 5, p2}, {6, p1, p2, 1, p1},
                {6, p1, p2, 4, p1p2}, {6, p1, p2, 2, p1p2},
                {5, p1, p2, 4, p2}, {5, p1, p2, 1, p1},
                {10, p1, p2, 4, p1p2}, {10, p1, p2, 1, p1p2},
        });
        final ArrayList<Object[]> params = new ArrayList<>();
        params.addAll(oneConflictArray);
        return params;
    }


    /**
     * Creates a set of #numNodes Paxos instances, and prepares a single threaded executor that serializes
     * messages to each instance (in line with how Rapid messages are serialized).
     */
    private Map<HostAndPort, FastPaxos> createNFastPaxosInstances(final int numNodes,
                                                                  final Consumer<List<HostAndPort>> onDecide) {
        final Map<HostAndPort, FastPaxos> instances = new ConcurrentHashMap<>();
        final Map<HostAndPort, ExecutorService> executorServiceMap = new ConcurrentHashMap<>();
        final DirectMessagingClient messagingClient = new DirectMessagingClient(instances, executorServiceMap);
        final DirectBroadcaster directBroadcaster = new DirectBroadcaster(instances, messagingClient);
        for (int i = 0; i < numNodes; i++) {
            final HostAndPort addr = HostAndPort.fromParts("127.0.0.1", 1234 + i);
            executorServiceMap.put(addr, Executors.newSingleThreadExecutor());
            final FastPaxos paxos = new FastPaxos(addr, 1, numNodes, messagingClient, directBroadcaster,
                                                  onDecide);
            instances.put(addr, paxos);
        }
        return instances;
    }

    /**
     * Directly wires Paxos messages to the instances.
     */
    private class DirectBroadcaster implements IBroadcaster {
        private final Map<HostAndPort, FastPaxos> paxosInstances;
        private final IMessagingClient messagingClient;

        DirectBroadcaster(final Map<HostAndPort, FastPaxos> paxosInstances,
                          final IMessagingClient messagingClient) {
            this.paxosInstances = paxosInstances;
            this.messagingClient = messagingClient;
        }

        @Override
        public List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest rapidRequest) {
            if (!messageTypeToDrop.contains(rapidRequest.getContentCase())) {
                paxosInstances.forEach((k, v) -> messagingClient.sendMessage(k, rapidRequest));
            }
            return Collections.emptyList();
        }

        @Override
        public void setMembership(final List<HostAndPort> recipients) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Directly wires Paxos messages to the instances.
     */
    private static class DirectMessagingClient implements IMessagingClient {
        private final Map<HostAndPort, FastPaxos> paxosInstances;
        private final Map<HostAndPort, ExecutorService> executors;

        DirectMessagingClient(final Map<HostAndPort, FastPaxos> paxosInstances,
                              final Map<HostAndPort, ExecutorService> executors) {
            this.paxosInstances = paxosInstances;
            this.executors = executors;
        }

        @Override
        public ListenableFuture<RapidResponse> sendMessage(final HostAndPort remote, final RapidRequest msg) {
            executors.get(remote).execute(() -> paxosInstances.get(remote).handleMessages(msg));
            return Futures.immediateFuture(RapidResponse.newBuilder()
                    .setConsensusResponse(ConsensusResponse.getDefaultInstance()).build());
        }

        @Override
        public ListenableFuture<RapidResponse> sendMessageBestEffort(final HostAndPort remote, final RapidRequest msg) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Wait and then verify all consensus decisions
     *
     * @param expectedSize expected size of each cluster
     * @param maxTries number of tries to checkMonitoree if the cluster has stabilized.
     * @param intervalInMs the time duration between checks.
     * @param decisions the reported consensus decisions
     */
    private void waitAndVerifyAgreement(final int expectedSize, final int maxTries, final int intervalInMs,
                                        final LinkedBlockingDeque<List<HostAndPort>> decisions)
                                        throws InterruptedException {
        int tries = maxTries;
        while (--tries > 0) {
            if (decisions.size() != expectedSize) {
                Thread.sleep(intervalInMs);
            }
            else {
                break;
            }
        }

        assertEquals(expectedSize, decisions.size());
        if (expectedSize > 0) {
            final List<HostAndPort> first = decisions.getFirst();
            assertAll(first, decisions);
        }
    }


    /**
     * Check if all values of a collection match
     */
    private void assertAll(final List<HostAndPort> value, final Collection<List<HostAndPort>> decisions) {
        for (final List<HostAndPort> decision : decisions) {
            assertEquals(value, decision);
        }
    }
}
