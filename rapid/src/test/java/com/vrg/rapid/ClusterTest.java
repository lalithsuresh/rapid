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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    @SuppressWarnings("FieldCanBeLocal")
    @Nullable private static Logger grpcLogger = null;
    private final ConcurrentHashMap<HostAndPort, Cluster> instances = new ConcurrentHashMap<>();
    private final int basePort = 1234;
    private final AtomicInteger portCounter = new AtomicInteger(basePort);

    @BeforeClass
    public static void beforeClass() {
        // gRPC INFO logs clutter the test output
        grpcLogger = Logger.getLogger("io.grpc");
        grpcLogger.setLevel(Level.WARNING);
    }

    @Before
    public void beforeTest() {
        instances.clear();

        // Tests need to opt out of the in-process channel
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        // Tests that depend on failure detection should set intervals by themselves
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 100000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 100000;
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
    @Test
    public void singleNodeJoinsThroughSeed() throws IOException, InterruptedException, ExecutionException {
        RpcServer.USE_IN_PROCESS_SERVER = false;
        RpcClient.USE_IN_PROCESS_CHANNEL = false;

        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(1, seedHost);
        verifyClusterSize(1, seedHost);
        extendCluster(1, seedHost);
        verifyClusterSize(2, seedHost);
    }

    /**
     * Test with K nodes joining the network through a single seed.
     */
    @Test
    public void tenNodesJoinSequentially() throws IOException, InterruptedException {
        // Explicitly test netty
        RpcServer.USE_IN_PROCESS_SERVER = false;
        RpcClient.USE_IN_PROCESS_CHANNEL = false;

        final int numNodes = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(1, seedHost); // Only bootstrap a seed.
        verifyClusterSize(1, seedHost);
        for (int i = 0; i < numNodes; i++) {
            extendCluster(1, seedHost);
            waitAndVerify( i + 2, 5, 1000, seedHost);
        }
    }

    /**
     * Identical to the previous test, but with more than K nodes joining in serial.
     */
    @Test
    public void twentyNodesJoinSequentially() throws IOException, InterruptedException {
        final int numNodes = 20;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(1, seedHost); // Only bootstrap a seed.
        verifyClusterSize(1, seedHost);

        for (int i = 0; i < numNodes; i++) {
            extendCluster(1, seedHost);
            waitAndVerify( i + 2, 5, 1000, seedHost);
        }
    }

    /**
     * Identical to the previous test, but with more than K nodes joining in parallel.
     * <p>
     * The test starts with a single seed and all N - 1 subsequent nodes initiate their join protocol at the same
     * time. This tests a single seed's ability to bootstrap a large cluster in one step.
     */
    @Test
    public void fiveHundredNodesJoinInParallel() throws IOException, InterruptedException {
        final int numNodes = 500; // Includes the size of the cluster
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyClusterSize(numNodes, seedHost);
    }


    /**
     * This test starts with a single seed, and a wave where 50 subsequent nodes initiate their join protocol
     * concurrently. Following this, a subsequent wave begins where 100 nodes then start together.
     */
    @Test
    public void hundredNodesJoinFiftyNodeCluster() throws IOException, InterruptedException {
        final int numNodesPhase1 = 50;
        final int numNodesPhase2 = 100;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodesPhase1, seedHost);
        waitAndVerify(numNodesPhase1, 10, 100, seedHost);
        extendCluster(numNodesPhase2, seedHost);
        waitAndVerify(numNodesPhase1 + numNodesPhase2, 10, 1000, seedHost);
    }

    /**
     * This test starts with a 4 node cluster. We then fail a single node to see if the monitoring mechanism
     * identifies the failing node and arrives at a decision to remove it.
     */
    @Test
    public void oneFailureOutOfFourNodes() throws IOException, InterruptedException {
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 1000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 1000;

        final int numNodes = 4;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyClusterSize(numNodes, seedHost);
        final HostAndPort nodeToFail = HostAndPort.fromParts("127.0.0.1", basePort + 2);
        failSomeNodes(Collections.singletonList(nodeToFail));
        waitAndVerify(numNodes - 1, 10, 1000, seedHost);
    }

    /**
     * This test starts with a 30 node cluster, then fails 5 nodes while an additional 10 join.
     */
    @Test
    public void concurrentNodeJoinsAndFails() throws IOException, InterruptedException {
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 3000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 1000;

        final int numNodes = 30;
        final int failingNodes = 5;
        final int phaseTwojoiners = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyClusterSize(numNodes, seedHost);
        failSomeNodes(IntStream.range(basePort + 2, basePort + 2 + failingNodes)
                               .mapToObj(i -> HostAndPort.fromParts("127.0.0.1", i))
                               .collect(Collectors.toList()));
        extendCluster(phaseTwojoiners, seedHost);
        waitAndVerify(numNodes - failingNodes + phaseTwojoiners, 20, 1000, seedHost);
    }

    /**
     * This test starts with a 5 node cluster, then joins two waves of six nodes each.
     */
    @Test
    public void concurrentNodeJoinsNetty() throws IOException, InterruptedException {
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 10000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 10000;
        RpcServer.USE_IN_PROCESS_SERVER = false;
        RpcClient.USE_IN_PROCESS_CHANNEL = false;
        final int numNodes = 5;
        final int phaseOneJoiners = 6;
        final int phaseTwojoiners = 6;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyClusterSize(numNodes, seedHost);
        final Random r = new Random();

        for (int i = 0; i < phaseOneJoiners/2; i++) {
            final List<HostAndPort> keysAsArray = new ArrayList<HostAndPort>(instances.keySet());
            extendCluster(2, keysAsArray.get(r.nextInt(instances.size())));
            Thread.sleep(50);
        }
        Thread.sleep(100);
        for (int i = 0; i < phaseTwojoiners; i++) {
            extendCluster(1, seedHost);
            Thread.sleep(50);
        }
        waitAndVerify(numNodes + phaseOneJoiners + phaseTwojoiners, 20, 1000, seedHost);
    }

    /**
     * This test starts with a 50 node cluster. We then fail 16 nodes to see if the monitoring mechanism
     * identifies the crashed nodes, and arrives at a decision.
     *
     */
    @Test
    public void sixteenFailuresOutOfFiftyNodes() throws IOException, InterruptedException {
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 3000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 1000;

        final int numNodes = 50;
        final int failingNodes = 16;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", basePort);
        createCluster(numNodes, seedHost);
        verifyClusterSize(numNodes, seedHost);
        failSomeNodes(IntStream.range(basePort + 2, basePort + 2 + failingNodes)
                .mapToObj(i -> HostAndPort.fromParts("127.0.0.1", i))
                .collect(Collectors.toList()));
        waitAndVerify(numNodes - failingNodes, 20, 1000, seedHost);
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
        final Cluster seed = new Cluster.Builder(seedHost).start();
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
     * @throws IOException Thrown if the Cluster.start() or join() methods throw an IOException when trying
     *                     to register an RpcServer.
     */
    private void extendCluster(final int numNodes, final HostAndPort seedHost) {
        final ExecutorService executor = Executors.newWorkStealingPool(numNodes);
        try {
            final CountDownLatch latch = new CountDownLatch(numNodes);
            for (int i = 0; i < numNodes; i++) {
                executor.submit(() -> {
                    try {
                        final HostAndPort joiningHost =
                                HostAndPort.fromParts("127.0.0.1", portCounter.incrementAndGet());
                        final Cluster nonSeed = new Cluster.Builder(joiningHost)
                                                           .join(seedHost);
                        instances.put(joiningHost, nonSeed);
                    } catch (final Exception e) {
                        e.printStackTrace();
                        fail();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
        } catch (final Exception e) {
            e.printStackTrace();
            fail();
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
        } catch (final Exception e) {
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
    private void verifyClusterSize(final int expectedSize, final HostAndPort seedHost) {
        assertEquals(expectedSize, instances.values().size());
        for (final Cluster cluster : instances.values()) {
            assertEquals(cluster.getMemberlist().size(), expectedSize);
            assertEquals(cluster.getMemberlist(), instances.get(seedHost).getMemberlist());
        }
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
    private void waitAndVerify(final int expectedSize, final int maxTries, final int intervalInMs,
                               final HostAndPort seedNode)
                                    throws InterruptedException {
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

        verifyClusterSize(expectedSize, seedNode);
    }
}