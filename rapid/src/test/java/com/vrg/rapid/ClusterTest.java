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
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.Charset;
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
    private final Map<Endpoint, Cluster> instances = new ConcurrentHashMap<>();
    private final Map<Endpoint, StaticFailureDetector.Factory> staticFds = new ConcurrentHashMap<>();
    private final Map<Endpoint, List<ServerDropInterceptors.FirstN>> serverInterceptors = new ConcurrentHashMap<>();
    private final Map<Endpoint, List<ClientInterceptors.Delayer>> clientInterceptors = new ConcurrentHashMap<>();
    private boolean useStaticFd = false;
    private boolean addMetadata = true;
    @Nullable private Random random = null;
    private long seed;
    private int basePort;
    @Nullable private AtomicInteger portCounter = null;
    private Settings settings = new Settings();

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
        settings = new Settings();

        // Tests need to opt out of the in-process channel
        settings.setUseInProcessTransport(true);
        // Tests need to set more aggressive frequent failure detection intervals if required
        settings.setFailureDetectorIntervalInMs(1000);
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
     * Verify public API that uses HostAndPort
     */
    @Test(timeout = 30000)
    public void hostAndPortBuilderTests() throws IOException, InterruptedException, ExecutionException {
        final HostAndPort addr1 = HostAndPort.fromParts("127.0.0.1", 1255);
        final HostAndPort addr2 = HostAndPort.fromParts("127.0.0.1", 1256);
        final Cluster seed = new Cluster.Builder(addr1).start();
        final Cluster joiner = new Cluster.Builder(addr2).join(addr1);
        assertEquals(2, seed.getMembershipSize());
        assertEquals(2, joiner.getMembershipSize());
        joiner.shutdown();
        seed.shutdown();
    }


    /**
     * Test with a single node joining through a seed.
     */
    @Test(timeout = 30000)
    public void singleNodeJoinsThroughSeed() throws IOException, InterruptedException, ExecutionException {
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(1, seedEndpoint);
        verifyCluster(1);
        extendCluster(1, seedEndpoint);
        verifyCluster(2);
    }

    /**
     * Test with K nodes joining the network through a single seed.
     */
    @Test(timeout = 30000)
    public void tenNodesJoinSequentially() throws IOException, InterruptedException {
        final int numNodes = 10;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(1, seedEndpoint); // Only bootstrap a seed.
        verifyCluster(1);
        for (int i = 0; i < numNodes; i++) {
            extendCluster(1, seedEndpoint);
            waitAndVerifyAgreement(i + 2, 5, 1000);
        }
    }

    /**
     * Identical to the previous test, but with more than K nodes joining in serial.
     */
    @Test(timeout = 30000)
    public void twentyNodesJoinSequentially() throws IOException, InterruptedException {
        final int numNodes = 20;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(1, seedEndpoint); // Only bootstrap a seed.
        verifyCluster(1);

        for (int i = 0; i < numNodes; i++) {
            extendCluster(1, seedEndpoint);
            waitAndVerifyAgreement(i + 2, 5, 1000);
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
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedEndpoint);
        verifyCluster(numNodes);
        verifyClusterMetadata(0);
    }

    /**
     * This test starts with a single seed, and a wave where 50 subsequent nodes initiate their join protocol
     * concurrently. Following this, a subsequent wave begins where 100 nodes then start together.
     */
    @Test(timeout = 30000)
    public void fiftyNodesJoinTwentyNodeCluster() throws IOException, InterruptedException {
        final int numNodesPhase1 = 20;
        final int numNodesPhase2 = 50;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodesPhase1, seedEndpoint);
        waitAndVerifyAgreement(numNodesPhase1, 10, 100);
        extendCluster(numNodesPhase2, seedEndpoint);
        waitAndVerifyAgreement(numNodesPhase1 + numNodesPhase2, 10, 1000);
    }

    /**
     * This test starts with a 4 node cluster. We then fail a single node to see if the monitoring mechanism
     * identifies the failing node and arrives at a decision to remove it.
     */
    @Test(timeout = 30000)
    public void oneFailureOutOfFiveNodes() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final int numNodes = 5;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedEndpoint);
        verifyCluster(numNodes);
        final Endpoint nodeToFail = Utils.hostFromParts("127.0.0.1", basePort + 2);
        failSomeNodes(Collections.singletonList(nodeToFail));
        waitAndVerifyAgreement(numNodes - 1, 10, 1000);
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
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedEndpoint);
        verifyCluster(numNodes);
        failSomeNodes(IntStream.range(basePort + 2, basePort + 2 + failingNodes)
                               .mapToObj(i -> Utils.hostFromParts("127.0.0.1", i))
                               .collect(Collectors.toList()));
        extendCluster(phaseTwojoiners, seedEndpoint);
        waitAndVerifyAgreement(numNodes - failingNodes + phaseTwojoiners, 20, 1000);
        verifyNumClusterInstances(numNodes - failingNodes + phaseTwojoiners);
    }

    /**
     * This test starts with a 5 node cluster, then joins two waves of six nodes each.
     */
    @Test(timeout = 30000)
    public void concurrentNodeJoinsNetty() throws IOException, InterruptedException {
        settings.setUseInProcessTransport(false);
        final int numNodes = 5;
        final int phaseOneJoiners = 6;
        final int phaseTwojoiners = 6;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedEndpoint);
        verifyCluster(numNodes);
        final Random r = new Random();

        for (int i = 0; i < phaseOneJoiners / 2; i++) {
            final List<Endpoint> keysAsArray = new ArrayList<>(instances.keySet());
            extendCluster(2, keysAsArray.get(r.nextInt(instances.size())));
        }
        for (int i = 0; i < phaseTwojoiners; i++) {
            extendCluster(1, seedEndpoint);
        }
        waitAndVerifyAgreement(numNodes + phaseOneJoiners + phaseTwojoiners, 20, 1000);
        verifyNumClusterInstances(numNodes + phaseOneJoiners + phaseTwojoiners);
    }

    /**
     * This test starts with a 50 node cluster. We then fail 12 nodes to see if the monitoring mechanism
     * identifies the crashed nodes, and arrives at a decision.
     *
     */
    @Test(timeout = 30000)
    public void failRandomQuarterOfNodes() throws IOException, InterruptedException {
        useStaticFd = true;
        final int numNodes = 50;
        final int numFailingNodes = 12;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedEndpoint);
        verifyCluster(numNodes);
        // Fail the first 3 nodes.
        final Set<Endpoint> failingNodes = getRandomHosts(numFailingNodes);
        staticFds.values().forEach(e -> e.addFailedNodes(failingNodes));
        failingNodes.forEach(h -> instances.remove(h).shutdown());
        waitAndVerifyAgreement(numNodes - failingNodes.size(), 20, 1000);
        // Nodes do not actually shutdown(), but are detected faulty. The faulty nodes have active
        // cluster instances and identify themselves as kicked out.
        verifyNumClusterInstances(numNodes - failingNodes.size());
    }


    /**
     * This test starts with a 50 node cluster. We then fail 16 nodes to see if the monitoring mechanism
     * identifies the crashed nodes, and arrives at a decision.
     *
     */
    @Test(timeout = 30000)
    public void failRandomThirdOfNodes() throws IOException, InterruptedException {
        useStaticFd = true;
        final int numNodes = 50;
        final int numFailingNodes = 16;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedEndpoint);
        verifyCluster(numNodes);
        // Fail the first 3 nodes.
        final Set<Endpoint> failingNodes = getRandomHosts(numFailingNodes);
        staticFds.values().forEach(e -> e.addFailedNodes(failingNodes));
        failingNodes.forEach(h -> instances.remove(h).shutdown());
        waitAndVerifyAgreement(numNodes - failingNodes.size(), 20, 1500);
        // Nodes do not actually shutdown(), but are detected faulty. The faulty nodes have active
        // cluster instances and identify themselves as kicked out.
        verifyNumClusterInstances(numNodes - failingNodes.size());
    }


    /**
     * This test starts with a 50 node cluster. We then use the static failure detector to fail
     * all edges to 10 nodes.
     */
    @Test(timeout = 30000)
    public void failTenRandomNodes() throws IOException, InterruptedException {
        useStaticFd = true;
        final int numNodes = 50;
        final int numFailingNodes = 10;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedEndpoint);
        verifyCluster(numNodes);
        // Fail the first 3 nodes.
        final Set<Endpoint> failingNodes = getRandomHosts(numFailingNodes);
        staticFds.values().forEach(e -> e.addFailedNodes(failingNodes));
        waitAndVerifyAgreement(numNodes - failingNodes.size(), 20, 1000);
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
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);

        // These nodes will drop the first 100 probe requests they receive
        final Set<Endpoint> failedNodes =
                getRandomHosts(basePort + 1, basePort + numNodes, numFailingNodes);
        // Since the random function returns a set of failed nodes,
        // we may have less than numFailedNodes entries in the set
        failedNodes.forEach(host -> dropFirstNAtServer(host, 100, RapidRequest.ContentCase.PROBEMESSAGE));
        createCluster(numNodes, seedEndpoint);
        waitAndVerifyAgreement(numNodes - failedNodes.size(), 10, 1000);
        verifyNumClusterInstances(numNodes);
    }

    /**
     * This test starts with a node joining a 1 node cluster. We drop phase 2 messages at the seed
     * such that RPC-level retries of the first join attempt eventually get through.
     */
    @Test(timeout = 30000)
    public void phase2MessageDropsRpcRetries() throws IOException, InterruptedException {
        useShortJoinTimeouts();
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        // Drop join-phase-2 attempts by nextNode, but only enough that the RPC retries make it past
        dropFirstNAtServer(seedEndpoint, (settings.getGrpcDefaultRetries()) - 1,
                RapidRequest.ContentCase.JOINMESSAGE);
        createCluster(1, seedEndpoint);
        extendCluster(1, seedEndpoint);
        waitAndVerifyAgreement(2, 15, 1000);
        verifyNumClusterInstances(2);
    }

    /**
     * This test starts with a node joining a 1 node cluster. We drop phase 2 messages at the seed
     * such that RPC-level retries of the first join attempt fail, and the client re-initiates a join.
     */
    @Test(timeout = 30000)
    public void phase2JoinAttemptRetry() throws IOException, InterruptedException {
        useShortJoinTimeouts();
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        // Drop join-phase-2 attempts by nextNode such that it re-attempts a join under a new settings
        dropFirstNAtServer(seedEndpoint, (settings.getGrpcDefaultRetries()) + 1,
                RapidRequest.ContentCase.JOINMESSAGE);
        createCluster(1, seedEndpoint);
        extendCluster(1, seedEndpoint);
        waitAndVerifyAgreement(2, 15, 1000);
        verifyNumClusterInstances(2);
    }

    /**
     * By the time a joiner issues a join-phase2-message, we change the settings.
     */
    @Test(timeout = 30000)
    public void phase2JoinAttemptRetryWithConfigChange() throws IOException, InterruptedException {
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        final Endpoint joinerEndpoint = Utils.hostFromParts("127.0.0.1", basePort + 1);
        // Drop join-phase-2 attempts by nextNode such that it re-attempts a join under a new settings
        createCluster(1, seedEndpoint);
        // The next host to join will have its join-phase2-message blocked.
        final CountDownLatch latch = blockAtClient(joinerEndpoint, RapidRequest.ContentCase.JOINMESSAGE);
        extendClusterNonBlocking(1, seedEndpoint);
        // The following node is now free to join. This will render the settings received by the previous
        // joiner node stale
        extendCluster(1, seedEndpoint);
        latch.countDown();
        waitAndVerifyAgreement(3, 15, 1000);
        verifyNumClusterInstances(3);
    }

    /**
     * Shutdown a node and rejoin multiple times.
     */
    @Test(timeout = 30000)
    public void testRejoinSingleNode() throws IOException, InterruptedException {
        useFastFailureDetectionTimeouts();
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        final Endpoint leavingEndpoint = Utils.hostFromParts("127.0.0.1", basePort + 1);
        createCluster(10, seedEndpoint);

        // Shutdown and rejoin twice
        for (int i = 0; i < 2; i++) {
            final Cluster cluster = instances.remove(leavingEndpoint);
            cluster.shutdown();
            waitAndVerifyAgreement(9, 20, 500);
            extendCluster(leavingEndpoint, seedEndpoint);
            waitAndVerifyAgreement(10, 20, 500);
        }
    }

    /**
     * Shutdown a node and rejoin before the failure detectors kick it out
     */
    @Test(timeout = 30000)
    public void testRejoinSingleNodeSameConfiguration() throws IOException, InterruptedException {
        useShortJoinTimeouts();
        useFastFailureDetectionTimeouts();
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        final Endpoint rejoiningEndpoint = Utils.hostFromParts("127.0.0.1", basePort + 1);
        createCluster(10, seedEndpoint);

        // Shutdown and rejoin once
        Cluster cluster = null;
        try {
            cluster = instances.remove(rejoiningEndpoint);
            cluster.shutdown();
            try {
                buildCluster(rejoiningEndpoint).join(seedEndpoint);
                fail();
            } catch (final Cluster.JoinException ignored) {
            }
            waitAndVerifyAgreement(9, 10, 1000);
            cluster = buildCluster(rejoiningEndpoint).join(seedEndpoint);
            waitAndVerifyAgreement(10, 10, 500);
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
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        final int numNodes = 30;
        final int failNodes = 5;
        createCluster(numNodes, seedEndpoint);

        final ExecutorService executor = Executors.newWorkStealingPool(failNodes);
        final CountDownLatch latch = new CountDownLatch(failNodes);
        for (int j = 0; j < failNodes; j++) {
            final int inc = j;
            executor.execute(() -> {
                // Shutdown and rejoin thrice
                try {
                    for (int i = 0; i < 3; i++) {
                        final Endpoint leavingEndpoint = Utils.hostFromParts("127.0.0.1", basePort + 1 + inc);
                        final Cluster cluster = instances.remove(leavingEndpoint);
                        try {
                            cluster.shutdown();
                            waitAndVerifyAgreement(numNodes - failNodes, 20, 500);
                            extendCluster(leavingEndpoint, seedEndpoint);
                            waitAndVerifyAgreement(numNodes, 20, 500);
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
        waitAndVerifyAgreement(numNodes, 10, 250);
        executor.shutdownNow();
    }

    /**
     * Test a node proactively leaving the cluster
     */
    @Test(timeout = 30000)
    public void testLeaving() throws IOException, InterruptedException {
        final int numNodes = 10;
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", basePort);
        createCluster(1, seedEndpoint); // Only bootstrap a seed.
        verifyCluster(1);
        for (int i = 0; i < numNodes; i++) {
            extendCluster(1, seedEndpoint);
            waitAndVerifyAgreement(i + 2, 5, 1000);
        }
        instances.get(seedEndpoint).leaveGracefully();
        instances.remove(seedEndpoint);
        waitAndVerifyAgreement(numNodes, 2, 1000);
    }

    /**
     * Creates a cluster of size {@code numNodes} with a seed {@code seedEndpoint}.
     *
     * @param numNodes cluster size
     * @param seedEndpoint Endpoint that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     * @throws IOException Thrown if the Cluster.start() or join() methods throw an IOException when trying
     *                     to register an RpcServer.
     */
    private void createCluster(final int numNodes, final Endpoint seedEndpoint) throws IOException {
        final Cluster seed = buildCluster(seedEndpoint).start();
        instances.put(seedEndpoint, seed);
        assertEquals(1, seed.getMemberlist().size());
        if (numNodes >= 2) {
            extendCluster(numNodes - 1, seedEndpoint);
        }
    }

    /**
     * Add {@code numNodes} instances to a cluster.
     *
     * @param numNodes cluster size
     * @param seedEndpoint Endpoint that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     */
    private void extendCluster(final int numNodes, final Endpoint seedEndpoint) {
        final ExecutorService executor = Executors.newWorkStealingPool(numNodes);
        try {
            final CountDownLatch latch = new CountDownLatch(numNodes);
            for (int i = 0; i < numNodes; i++) {
                executor.execute(() -> {
                    try {
                        final Endpoint joiningEndpoint =
                                Utils.hostFromParts("127.0.0.1", portCounter.incrementAndGet());
                        final Cluster nonSeed = buildCluster(joiningEndpoint).join(seedEndpoint);
                        instances.put(joiningEndpoint, nonSeed);
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
     * @param joiningNode Endpoint that represents the node joining.
     * @param seedEndpoint Endpoint that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     */
    private void extendCluster(final Endpoint joiningNode, final Endpoint seedEndpoint) {
        final ExecutorService executor = Executors.newWorkStealingPool(1);
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            executor.execute(() -> {
                try {
                    final Cluster nonSeed = buildCluster(joiningNode).join(seedEndpoint);
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
     * @param seedEndpoint Endpoint that represents the seed node to initialize and be used as the contact point
     *                 for subsequent joiners.
     */
    private void extendClusterNonBlocking(final int numNodes, final Endpoint seedEndpoint) {
        final ExecutorService executor = Executors.newWorkStealingPool(numNodes);
        try {
            for (int i = 0; i < numNodes; i++) {
                executor.execute(() -> {
                    try {
                        final Endpoint joiningEndpoint =
                                Utils.hostFromParts("127.0.0.1", portCounter.incrementAndGet());
                        final Cluster nonSeed = buildCluster(joiningEndpoint).join(seedEndpoint);
                        instances.put(joiningEndpoint, nonSeed);
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
     * @param nodesToFail list of Endpoint objects representing the nodes to fail
     */
    private void failSomeNodes(final List<Endpoint> nodesToFail) {
        final ExecutorService executor = Executors.newWorkStealingPool(nodesToFail.size());
        try {
            final CountDownLatch latch = new CountDownLatch(nodesToFail.size());
            for (final Endpoint nodeToFail : nodesToFail) {
                executor.execute(() -> {
                    try {
                        assertTrue(nodeToFail + " not in instances", instances.containsKey(nodeToFail));
                        instances.get(nodeToFail).shutdown();
                        instances.remove(nodeToFail);
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
     */
    private void verifyCluster(final int expectedSize) {
        final List<Endpoint> any = instances.entrySet().iterator().next().getValue().getMemberlist();
        for (final Cluster cluster : instances.values()) {
            assertEquals(cluster.toString(), expectedSize, cluster.getMemberlist().size());
            assertEquals(cluster.getMemberlist(), any);
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
     * @param maxTries number of tries to checkSubject if the cluster has stabilized.
     * @param intervalInMs the time duration between checks.
     */
    private void waitAndVerifyAgreement(final int expectedSize, final int maxTries, final int intervalInMs)
            throws InterruptedException {
        int tries = maxTries;
        while (--tries > 0) {
            boolean ready = true;
            final List<Endpoint> any = instances.entrySet().iterator().next().getValue().getMemberlist();
            for (final Cluster cluster : instances.values()) {
                if (!(cluster.getMemberlist().size() == expectedSize
                        && cluster.getMemberlist().equals(any))) {
                    ready = false;
                }
            }
            if (!ready) {
                Thread.sleep(intervalInMs);
            } else {
                break;
            }
        }

        verifyCluster(expectedSize);
    }

    // Helper that provides a list of N random nodes that have already been added to the instances map
    private Set<Endpoint> getRandomHosts(final int N) {
        assert random != null;
        final List<Map.Entry<Endpoint, Cluster>> entries = new ArrayList<>(instances.entrySet());
        Collections.shuffle(entries);
        return random.ints(instances.size(), 0, N)
                     .mapToObj(i -> entries.get(i).getKey())
                     .collect(Collectors.toSet());
    }

    // Helper that provides a list of N random nodes from portStart to portEnd
    private Set<Endpoint> getRandomHosts(final int portStart, final int portEnd, final int N) {
        assert random != null;
        return random.ints(N, portStart, portEnd)
                .mapToObj(i -> Utils.hostFromParts("127.0.0.1", i))
                .collect(Collectors.toSet());
    }

    // Helper to use static-failure-detectors and inject interceptors
    private Cluster.Builder buildCluster(final Endpoint endpoint) {
        Cluster.Builder builder = new Cluster.Builder(endpoint).useSettings(settings);
        if (useStaticFd) {
            final StaticFailureDetector.Factory fdFactory = new StaticFailureDetector.Factory(new HashSet<>());
            builder = builder.setEdgeFailureDetectorFactory(fdFactory);
            staticFds.put(endpoint, fdFactory);
        }
        if (serverInterceptors.containsKey(endpoint)) {
            builder = builder.setMessagingClientAndServer(new GrpcClient(endpoint, settings),
                                                          new TestingGrpcServer(endpoint,
                                                          serverInterceptors.get(endpoint),
                                                                  settings.getUseInProcessTransport()));
        }
        if (clientInterceptors.containsKey(endpoint)) {
            builder = builder.setMessagingClientAndServer(new TestingGrpcClient(endpoint, settings,
                                                                                clientInterceptors.get(endpoint)),
                    new TestingGrpcServer(endpoint,
                            Collections.emptyList(),
                            settings.getUseInProcessTransport()));
        }
        if (addMetadata) {
            final ByteString byteString = ByteString.copyFrom(endpoint.toString(), Charset.defaultCharset());
            builder = builder.setMetadata(Collections.singletonMap("Key", byteString));
        }

        return builder;
    }

    // Helper that drops the first N requests at a server of a given type
    private <T, E> void dropFirstNAtServer(final Endpoint endpoint, final int N,
                                           final RapidRequest.ContentCase contentCase) {
        serverInterceptors.computeIfAbsent(endpoint, (k) -> new ArrayList<>(1))
                .add(new ServerDropInterceptors.FirstN(N, contentCase));
    }

    // Helper that delays requests of a given type at the client
    private <T, E> CountDownLatch blockAtClient(final Endpoint endpoint, final RapidRequest.ContentCase messageType) {
        final CountDownLatch latch = new CountDownLatch(1);
        clientInterceptors.computeIfAbsent(endpoint, (k) -> new ArrayList<>(1))
                .add(new ClientInterceptors.Delayer(latch, messageType));
        return latch;
    }

    // This speeds up the retry attempts during the join protocol
    private void useShortJoinTimeouts() {
        settings.setGrpcTimeoutMs(100);
        settings.setGrpcJoinTimeoutMs(500); // use short timeouts
    }

    // This speeds up failure detection when using the PingPongFailureDetector
    private void useFastFailureDetectionTimeouts() {
        settings.setGrpcProbeTimeoutMs(10);
        settings.setFailureDetectorIntervalInMs(50);
    }
}