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
import com.vrg.rapid.pb.MembershipServiceGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test public API
 */
public class ClusterTest {
    private static final Logger GRPC_LOGGER;
    private static final Logger NETTY_LOGGER;
    private final Map<HostAndPort, Cluster> instances = new ConcurrentHashMap<>();
    private final Map<HostAndPort, StaticFailureDetector> staticFds = new ConcurrentHashMap<>();
    private final Map<HostAndPort, List<ServerInterceptor>> serverInterceptors = new ConcurrentHashMap<>();
    private final Map<HostAndPort, List<ClientInterceptor>> clientInterceptors = new ConcurrentHashMap<>();
    private boolean useStaticFd = false;
    private boolean addMetadata = true;
    @Nullable private Random random = null;
    private long seed;
    private int basePort;
    @Nullable private AtomicInteger portCounter = null;
    private RpcClient.Conf clientConf = new RpcClient.Conf();

    static {
        // gRPC and netty logs clutter the test output
        GRPC_LOGGER = Logger.getLogger("io.grpc");
        GRPC_LOGGER.setLevel(Level.OFF);
        NETTY_LOGGER = Logger.getLogger("io.grpc.netty.NettyServerHandler");
        NETTY_LOGGER.setLevel(Level.OFF);
    }

    @Rule
    public final TestWatcher testWatcher = new TestWatcher() {
        @Override
        protected void failed(final Throwable e, final Description description) {

            System.out.println("\u001B[31m     [FAILED] [Seed: " + seed + "] " + description.getMethodName()
                               + "\u001B[0m");
        }
    };

    @Before
    public void beforeTest() {
        basePort =  1234;
        portCounter = new AtomicInteger(basePort);
        instances.clear();
        seed = ThreadLocalRandom.current().nextLong();
        random = new Random(seed);
        clientConf = new RpcClient.Conf();

        // Tests need to opt out of the in-process channel
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 1000;
        useStaticFd = false;
        addMetadata = true;
        staticFds.clear();
        serverInterceptors.clear();
        clientInterceptors.clear();
    }

    @After
    public void afterTest() throws InterruptedException {
        for (final Cluster cluster: instances.values()) {
            cluster.shutdown();
        }
    }

    /**
     * Test with a single node joining through a seed.
     */
    @Test(timeout = 30000)
    public void singleNodeJoinsThroughSeed() throws IOException, InterruptedException, ExecutionException {
        RpcServer.USE_IN_PROCESS_SERVER = false;
        RpcClient.USE_IN_PROCESS_CHANNEL = false;

        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(1, seedHost);
        verifyCluster(1, seedHost);
        extendCluster(1, seedHost);
        verifyCluster(2, seedHost);
    }

    /**
     * Test with K nodes joining the network through a single seed.
     */
    @Test(timeout = 30000)
    public void tenNodesJoinSequentially() throws IOException, InterruptedException {
        // Explicitly test netty
        RpcServer.USE_IN_PROCESS_SERVER = false;
        RpcClient.USE_IN_PROCESS_CHANNEL = false;

        final int numNodes = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(1, seedHost); // Only bootstrap a seed.
        verifyCluster(1, seedHost);
        for (int i = 0; i < numNodes; i++) {
            extendCluster(1, seedHost);
            waitAndVerifyAgreement(i + 2, 5, 1000, seedHost);
        }
    }

    /**
     * Identical to the previous test, but with more than K nodes joining in serial.
     */
    @Test(timeout = 30000)
    public void twentyNodesJoinSequentially() throws IOException, InterruptedException {
        // Explicitly test netty
        RpcServer.USE_IN_PROCESS_SERVER = false;
        RpcClient.USE_IN_PROCESS_CHANNEL = false;

        final int numNodes = 20;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(1, seedHost); // Only bootstrap a seed.
        verifyCluster(1, seedHost);

        for (int i = 0; i < numNodes; i++) {
            extendCluster(1, seedHost);
            waitAndVerifyAgreement(i + 2, 5, 1000, seedHost);
        }
    }

    /**
     * Identical to the previous test, but with more than K nodes joining in parallel.
     *
     * The test starts with a single seed and all N - 1 subsequent nodes initiate their join protocol at the same
     * time. This tests a single seed's ability to bootstrap a large cluster in one step.
     */
    @Test(timeout = 150000)
    public void hundredNodesJoinInParallel() throws IOException, InterruptedException {
        addMetadata = false;
        final int numNodes = 100; // Includes the size of the cluster
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyCluster(numNodes, seedHost);
        verifyClusterMetadata(0);
    }

    /**
     * This test starts with a single seed, and a wave where 50 subsequent nodes initiate their join protocol
     * concurrently. Following this, a subsequent wave begins where 100 nodes then start together.
     */
    @Test(timeout = 150000)
    public void fiftyNodesJoinTwentyNodeCluster() throws IOException, InterruptedException {
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        final int numNodesPhase1 = 20;
        final int numNodesPhase2 = 50;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodesPhase1, seedHost);
        waitAndVerifyAgreement(numNodesPhase1, 10, 100, seedHost);
        extendCluster(numNodesPhase2, seedHost);
        waitAndVerifyAgreement(numNodesPhase1 + numNodesPhase2, 10, 1000, seedHost);
    }

    /**
     * This test starts with a 4 node cluster. We then fail a single node to see if the monitoring mechanism
     * identifies the failing node and arrives at a decision to remove it.
     */
    @Test(timeout = 30000)
    public void oneFailureOutOfFiveNodes() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final int numNodes = 5;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyCluster(numNodes, seedHost);
        final HostAndPort nodeToFail = HostAndPort.fromParts("127.0.0.1", basePort + 2);
        failSomeNodes(Collections.singletonList(nodeToFail));
        waitAndVerifyAgreement(numNodes - 1, 10, 1000, seedHost);
        verifyNumClusterInstances(numNodes - 1);
    }

    /**
     * This test starts with a 30 node cluster, then fails 5 nodes while an additional 10 join.
     */
    @Test(timeout = 30000)
    public void concurrentNodeJoinsAndFails() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final int numNodes = 30;
        final int failingNodes = 5;
        final int phaseTwojoiners = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyCluster(numNodes, seedHost);
        failSomeNodes(IntStream.range(basePort + 2, basePort + 2 + failingNodes)
                               .mapToObj(i -> HostAndPort.fromParts("127.0.0.1", i))
                               .collect(Collectors.toList()));
        extendCluster(phaseTwojoiners, seedHost);
        waitAndVerifyAgreement(numNodes - failingNodes + phaseTwojoiners, 20, 1000, seedHost);
        verifyNumClusterInstances(numNodes - failingNodes + phaseTwojoiners);
    }

    /**
     * This test starts with a 5 node cluster, then joins two waves of six nodes each.
     */
    @Test(timeout = 30000)
    public void concurrentNodeJoinsNetty() throws IOException, InterruptedException {
        RpcServer.USE_IN_PROCESS_SERVER = false;
        RpcClient.USE_IN_PROCESS_CHANNEL = false;
        final int numNodes = 5;
        final int phaseOneJoiners = 6;
        final int phaseTwojoiners = 6;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyCluster(numNodes, seedHost);
        final Random r = new Random();

        for (int i = 0; i < phaseOneJoiners / 2; i++) {
            final List<HostAndPort> keysAsArray = new ArrayList<>(instances.keySet());
            extendCluster(2, keysAsArray.get(r.nextInt(instances.size())));
        }
        Thread.sleep(100);
        for (int i = 0; i < phaseTwojoiners; i++) {
            extendCluster(1, seedHost);
        }
        waitAndVerifyAgreement(numNodes + phaseOneJoiners + phaseTwojoiners, 20, 1000, seedHost);
        verifyNumClusterInstances(numNodes + phaseOneJoiners + phaseTwojoiners);
    }

    /**
     * This test starts with a 50 node cluster. We then fail 16 nodes to see if the monitoring mechanism
     * identifies the crashed nodes, and arrives at a decision.
     *
     */
    @Test(timeout = 30000)
    public void twelveFailuresOutOfFiftyNodes() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final int numNodes = 50;
        final int failingNodes = 12;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyCluster(numNodes, seedHost);
        failSomeNodes(IntStream.range(basePort + 2, basePort + 2 + failingNodes)
                .mapToObj(i -> HostAndPort.fromParts("127.0.0.1", i))
                .collect(Collectors.toList()));
        waitAndVerifyAgreement(numNodes - failingNodes, 30, 1000, seedHost);
        verifyNumClusterInstances(numNodes - failingNodes);
    }

    /**
     * This test starts with a 50 node cluster. We then use the static failure detector to fail
     * all edges to 3 nodes.
     */
    @Test(timeout = 30000)
    public void failTenRandomNodes() throws IOException, InterruptedException {
        useStaticFd = true;
        final int numNodes = 50;
        final int numFailingNodes = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyCluster(numNodes, seedHost);
        // Fail the first 3 nodes.
        final Set<HostAndPort> failingNodes = getRandomHosts(numFailingNodes);
        staticFds.values().forEach(e -> e.addFailedNodes(failingNodes));
        waitAndVerifyAgreement(numNodes - failingNodes.size(), 20, 1000, seedHost);
        // Nodes do not actually shutdown(), but are detected faulty. The faulty nodes have active
        // cluster instances and identify themselves as kicked out.
        verifyNumClusterInstances(numNodes);
    }

    /**
     * This test starts with a 50 node cluster. We then randomly fail at most 10 randomly selected nodes.
     */
    @Test
    public void injectAsymmetricDrops() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final int numNodes = 50;
        final int numFailingNodes = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);

        // These nodes will drop the first 100 probe requests they receive
        final Set<HostAndPort> failedNodes =
                getRandomHosts(basePort + 1, basePort + numNodes, numFailingNodes);
        // Since the random function returns a set of failed nodes,
        // we may have less than numFailedNodes entries in the set
        failedNodes.forEach(host -> dropFirstNAtServer(host, 100, MembershipServiceGrpc.METHOD_RECEIVE_PROBE));
        createCluster(numNodes, seedHost);
        waitAndVerifyAgreement(numNodes - failedNodes.size(), 10, 1000, seedHost);
        verifyNumClusterInstances(numNodes);
    }

    /**
     * This test starts with a node joining a 1 node cluster. We drop phase 2 messages at the seed
     * such that RPC-level retries of the first join attempt eventually get through.
     */
    @Test(timeout = 30000)
    public void phase2MessageDropsRpcRetries() throws IOException, InterruptedException {
        useShortJoinTimeouts();
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        // Drop join-phase-2 attempts by nextNode, but only enough that the RPC retries make it past
        dropFirstNAtServer(seedHost, (clientConf.RPC_DEFAULT_RETRIES) - 1,
                   MembershipServiceGrpc.METHOD_RECEIVE_JOIN_PHASE2MESSAGE);
        createCluster(1, seedHost);
        extendCluster(1, seedHost);
        waitAndVerifyAgreement(2, 15, 1000, seedHost);
        verifyNumClusterInstances(2);
    }

    /**
     * This test starts with a node joining a 1 node cluster. We drop phase 2 messages at the seed
     * such that RPC-level retries of the first join attempt fail, and the client re-initiates a join.
     */
    @Test(timeout = 30000)
    public void phase2JoinAttemptRetry() throws IOException, InterruptedException {
        useShortJoinTimeouts();
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        // Drop join-phase-2 attempts by nextNode such that it re-attempts a join under a new configuration
        dropFirstNAtServer(seedHost, (clientConf.RPC_DEFAULT_RETRIES) + 1,
                   MembershipServiceGrpc.METHOD_RECEIVE_JOIN_PHASE2MESSAGE);
        createCluster(1, seedHost);
        extendCluster(1, seedHost);
        waitAndVerifyAgreement(2, 15, 1000, seedHost);
        verifyNumClusterInstances(2);
    }

    /**
     * By the time a joiner issues a join-phase2-message, we change the configuration.
     */
    @Test(timeout = 30000)
    public void phase2JoinAttemptRetryWithConfigChange() throws IOException, InterruptedException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        final HostAndPort joinerHost = HostAndPort.fromParts("127.0.0.1", basePort + 1);
        // Drop join-phase-2 attempts by nextNode such that it re-attempts a join under a new configuration
        createCluster(1, seedHost);
        // The next host to join will have its join-phase2-message blocked.
        final CountDownLatch latch = blockAtClient(joinerHost, MembershipServiceGrpc.METHOD_RECEIVE_JOIN_PHASE2MESSAGE);
        extendClusterNonBlocking(1, seedHost);
        // The following node is now free to join. This will render the configuration received by the previous
        // joiner node stale
        extendCluster(1, seedHost);
        latch.countDown();
        waitAndVerifyAgreement(3, 15, 1000, seedHost);
        verifyNumClusterInstances(3);
    }

    /**
     * Shutdown a node and rejoin multiple times.
     */
    @Test(timeout = 30000)
    public void testRejoinSingleNode() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        final HostAndPort leavingHost = HostAndPort.fromParts("127.0.0.1", basePort + 1);
        createCluster(10, seedHost);

        // Shutdown and rejoin twice
        for (int i = 0; i < 2; i++) {
            final Cluster cluster = instances.remove(leavingHost);
            cluster.shutdown();
            waitAndVerifyAgreement(9, 20, 500, seedHost);
            extendCluster(leavingHost, seedHost);
            waitAndVerifyAgreement(10, 20, 500, seedHost);
        }
    }

    /**
     * Shutdown a node and rejoin before the failure detectors kick it out
     */
    @Test(timeout = 30000)
    public void testRejoinSingleNodeSameConfiguration() throws IOException, InterruptedException {
        useShortJoinTimeouts();
        useFastFailureDetectionTimeouts();
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        final HostAndPort rejoiningHost = HostAndPort.fromParts("127.0.0.1", basePort + 1);
        createCluster(10, seedHost);

        // Shutdown and rejoin once
        Cluster cluster = null;
        try {
            cluster = instances.remove(rejoiningHost);
            cluster.shutdown();
            try {
                buildCluster(rejoiningHost).join(seedHost);
                fail();
            } catch (final Cluster.JoinException ignored) {
            }
            waitAndVerifyAgreement(9, 10, 1000, seedHost);
            cluster = buildCluster(rejoiningHost).join(seedHost);
            waitAndVerifyAgreement(10, 10, 500, seedHost);
        } finally {
            if (cluster != null) {
                cluster.shutdown();
            }
        }
    }

    /**
     * Shutdown a node and rejoin multiple times.
     */
    @Test(timeout = 30000)
    public void testRejoinMultipleNodes() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        final int numNodes = 30;
        final int failNodes = 5;
        createCluster(numNodes, seedHost);

        final ExecutorService executor = Executors.newWorkStealingPool(failNodes);
        final CountDownLatch latch = new CountDownLatch(failNodes);
        for (int j = 0; j < failNodes; j++) {
            final int inc = j;
            executor.execute(() -> {
                // Shutdown and rejoin thrice
                try {
                    for (int i = 0; i < 3; i++) {
                        final HostAndPort leavingHost = HostAndPort.fromParts("127.0.0.1", basePort + 1 + inc);
                        final Cluster cluster = instances.remove(leavingHost);
                        try {
                            cluster.shutdown();
                            waitAndVerifyAgreement(numNodes - failNodes, 20, 500, seedHost);
                            extendCluster(leavingHost, seedHost);
                            waitAndVerifyAgreement(numNodes, 20, 500, seedHost);
                        } catch (final InterruptedException e) {
                            fail();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        waitAndVerifyAgreement(numNodes, 10, 250, seedHost);
    }

    /**
     * Creates a cluster of size {@code numNodes} with a seed {@code seedHost}.
     *
     * @param numNodes cluster size
     * @param seedHost HostAndPort that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     * @throws IOException Thrown if the Cluster.start() or join() methods throw an IOException when trying
     *                     to register an RpcServer.
     */
    private void createCluster(final int numNodes, final HostAndPort seedHost) throws IOException {
        final Cluster seed = buildCluster(seedHost).start();
        instances.put(seedHost, seed);
        assertEquals(seed.getMemberlist().size(), 1);
        if (numNodes >= 2) {
            extendCluster(numNodes - 1, seedHost);
        }
    }

    /**
     * Add {@code numNodes} instances to a cluster.
     *
     * @param numNodes cluster size
     * @param seedHost HostAndPort that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     */
    private void extendCluster(final int numNodes, final HostAndPort seedHost) {
        final ExecutorService executor = Executors.newWorkStealingPool(numNodes);
        try {
            final CountDownLatch latch = new CountDownLatch(numNodes);
            for (int i = 0; i < numNodes; i++) {
                executor.execute(() -> {
                    try {
                        final HostAndPort joiningHost =
                                HostAndPort.fromParts("127.0.0.1", portCounter.incrementAndGet());
                        final Cluster nonSeed = buildCluster(joiningHost).join(seedHost);
                        instances.put(joiningHost, nonSeed);
                    } catch (final InterruptedException | IOException e) {
                        e.printStackTrace();
                        fail();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
        } catch (final InterruptedException e) {
            e.printStackTrace();
            fail();
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Add {@code numNodes} instances to a cluster.
     *
     * @param joiningNode HostAndPort that represents the node joining.
     * @param seedHost HostAndPort that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     */
    private void extendCluster(final HostAndPort joiningNode, final HostAndPort seedHost) {
        final ExecutorService executor = Executors.newWorkStealingPool(1);
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            executor.execute(() -> {
                try {
                    final Cluster nonSeed = buildCluster(joiningNode).join(seedHost);
                    instances.put(joiningNode, nonSeed);
                } catch (final InterruptedException | IOException e) {
                    fail();
                } finally {
                    latch.countDown();
                }
            });
            latch.await();
        } catch (final InterruptedException e) {
            e.printStackTrace();
            fail();
        } finally {
            executor.shutdown();
        }
    }


    /**
     * Add {@code numNodes} instances to a cluster without waiting for their join methods to return
     *
     * @param numNodes cluster size
     * @param seedHost HostAndPort that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     */
    private void extendClusterNonBlocking(final int numNodes, final HostAndPort seedHost) {
        final ExecutorService executor = Executors.newWorkStealingPool(numNodes);
        try {
            for (int i = 0; i < numNodes; i++) {
                executor.execute(() -> {
                    try {
                        final HostAndPort joiningHost =
                                HostAndPort.fromParts("127.0.0.1", portCounter.incrementAndGet());
                        final Cluster nonSeed = buildCluster(joiningHost).join(seedHost);
                        instances.put(joiningHost, nonSeed);
                    } catch (final InterruptedException | IOException e) {
                        e.printStackTrace();
                        fail();
                    }
                });
            }
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Fail a set of nodes in a cluster by calling shutdown().
     *
     * @param nodesToFail list of HostAndPort objects representing the nodes to fail
     */
    private void failSomeNodes(final List<HostAndPort> nodesToFail) {
        final ExecutorService executor = Executors.newWorkStealingPool(nodesToFail.size());
        try {
            final CountDownLatch latch = new CountDownLatch(nodesToFail.size());
            for (final HostAndPort nodeToFail : nodesToFail) {
                executor.execute(() -> {
                    try {
                        assertTrue(nodeToFail + " not in instances", instances.containsKey(nodeToFail));
                        instances.get(nodeToFail).shutdown();
                        instances.remove(nodeToFail);
                    } catch (final InterruptedException e) {
                        fail();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
        } catch (final InterruptedException e) {
            e.printStackTrace();
            fail();
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Verify that all nodes in the cluster are of size {@code expectedSize} and have an identical
     * list of members as the seed node.
     *
     * @param expectedSize expected size of each cluster
     * @param seedHost seed node to validate the cluster view against
     */
    private void verifyCluster(final int expectedSize, final HostAndPort seedHost) {
        for (final Cluster cluster : instances.values()) {
            assertEquals(cluster.toString(), expectedSize, cluster.getMemberlist().size());
            assertEquals(cluster.getMemberlist(), instances.get(seedHost).getMemberlist());
            if (addMetadata) {
                assertEquals(cluster.toString(), expectedSize, cluster.getClusterMetadata().size());
            }
        }
    }

    /**
     * Verify that all nodes in the cluster are of size {@code expectedSize} and have an identical
     * list of members as the seed node.
     *
     * @param expectedSize expected size of each cluster
     */
    private void verifyClusterMetadata(final int expectedSize) {
        for (final Cluster cluster : instances.values()) {
            assertEquals(cluster.getClusterMetadata().size(), expectedSize);
        }
    }

    /**
     * Verify the number of Cluster instances that managed to start.
     *
     * @param expectedSize expected size of each cluster
     */
    private void verifyNumClusterInstances(final int expectedSize) {
        assertEquals(expectedSize, instances.size());
    }

    /**
     * Verify whether the cluster has converged {@code maxTries} times with a delay of @{code intervalInMs}
     * between attempts. This is used to give failure detector logic some time to kick in.
     *
     * @param expectedSize expected size of each cluster
     * @param maxTries number of tries to checkMonitoree if the cluster has stabilized.
     * @param intervalInMs the time duration between checks.
     * @param seedNode the seed node to validate the cluster membership against
     */
    private void waitAndVerifyAgreement(final int expectedSize, final int maxTries, final int intervalInMs,
                                        final HostAndPort seedNode) throws InterruptedException {
        int tries = maxTries;
        while (--tries > 0) {
            boolean ready = true;
            for (final Cluster cluster : instances.values()) {
                if (!(cluster.getMemberlist().size() == expectedSize
                        && cluster.getMemberlist().equals(instances.get(seedNode).getMemberlist()))) {
                    ready = false;
                }
            }
            if (!ready) {
                Thread.sleep(intervalInMs);
            } else {
                break;
            }
        }

        verifyCluster(expectedSize, seedNode);
    }

    // Helper that provides a list of N random nodes that have already been added to the instances map
    private Set<HostAndPort> getRandomHosts(final int N) {
        assert random != null;
        final List<Map.Entry<HostAndPort, Cluster>> entries = new ArrayList<>(instances.entrySet());
        Collections.shuffle(entries);
        return random.ints(instances.size(), 0, N)
                     .mapToObj(i -> entries.get(i).getKey())
                     .collect(Collectors.toSet());
    }

    // Helper that provides a list of N random nodes from portStart to portEnd
    private Set<HostAndPort> getRandomHosts(final int portStart, final int portEnd, final int N) {
        assert random != null;
        return random.ints(N, portStart, portEnd)
                .mapToObj(i -> HostAndPort.fromParts("127.0.0.1", i))
                .collect(Collectors.toSet());
    }

    // Helper to use static-failure-detectors and inject interceptors
    private Cluster.Builder buildCluster(final HostAndPort host) {
        Cluster.Builder builder = new Cluster.Builder(host).setRpcClientConf(clientConf);
        if (useStaticFd) {
            final StaticFailureDetector fd = new StaticFailureDetector(new HashSet<>());
            builder = builder.setLinkFailureDetector(fd);
            staticFds.put(host, fd);
        }
        if (serverInterceptors.containsKey(host)) {
            builder = builder.setServerInterceptors(serverInterceptors.get(host));
        }
        if (clientInterceptors.containsKey(host)) {
            builder = builder.setClientInterceptors(clientInterceptors.get(host));
        }
        if (addMetadata) {
            builder = builder.setMetadata(Collections.singletonMap("Key", host.toString()));
        }
        return builder;
    }

    // Helper that drops the first N requests at a server of a given type
    private <T, E> void dropFirstNAtServer(final HostAndPort host, final int N,
                                           final MethodDescriptor<T, E> messageType) {
        serverInterceptors.computeIfAbsent(host, (k) -> new ArrayList<>(1))
                .add(new ServerDropInterceptors.FirstN<>(N, messageType));
    }

    // Helper that delays requests of a given type at the client
    private <T, E> CountDownLatch blockAtClient(final HostAndPort host, final MethodDescriptor<T, E> messageType) {
        final CountDownLatch latch = new CountDownLatch(1);
        clientInterceptors.computeIfAbsent(host, (k) -> new ArrayList<>(1))
                .add(new ClientInterceptors.Delayer<>(latch, messageType));
        return latch;
    }

    // This speeds up the retry attempts during the join protocol
    private void useShortJoinTimeouts() {
        clientConf.RPC_TIMEOUT_MS = 100;
        clientConf.RPC_JOIN_PHASE_2_TIMEOUT = 500; // use short timeouts
    }

    // This speeds up failure detection when using the PingPongFailureDetector
    private void useFastFailureDetectionTimeouts() {
        clientConf.RPC_PROBE_TIMEOUT = 200;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 200;
    }
}