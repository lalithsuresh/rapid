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
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test public API
 */
public class ClusterTest {

    static {
        // gRPC INFO logs clutter the test output
        Logger.getLogger("io.grpc").setLevel(Level.WARNING);
    }

    /**
     * Test with a single node joining through a seed.
     */
    @Test
    public void testSingleJoin() throws IOException, InterruptedException, ExecutionException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final HostAndPort joiningHost = HostAndPort.fromParts("127.0.0.1", 1235);

        final Cluster seed = Cluster.start(seedHost);
        final Cluster nonSeed = Cluster.join(seedHost, joiningHost);

        Thread.sleep(1000);
        try {
            assertEquals(2, seed.getMemberlist().size());
        }
        finally {
            seed.shutdown();
            nonSeed.shutdown();
        }
    }

    /**
     * Test with K nodes joining the network through a single seed.
     */
    @Test
    public void testJoinTenSequential() throws IOException, InterruptedException {
        final int numNodes = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final List<Cluster> serviceList = new ArrayList<>();
        final Cluster seed = Cluster.start(seedHost);
        serviceList.add(seed);
        try {
            for (int i = 0; i < numNodes; i++) {
                final HostAndPort joiningHost = HostAndPort.fromParts("127.0.0.1", 1235 + i);
                final Cluster nonSeed = Cluster.join(seedHost, joiningHost);
                serviceList.add(nonSeed);
                Thread.sleep(50);
                assertEquals(i + 2, nonSeed.getMemberlist().size());
            }
        }
        catch (final InterruptedException | RuntimeException e) {
            e.printStackTrace();
            fail();
        }
        finally {
            for (final Cluster service: serviceList) {
                service.shutdown();
            }
        }
    }


    /**
     * Identical to the previous test, but with more than K nodes joining in serial.
     */
    @Test
    public void testJoinMoreThanKSequential() throws IOException, InterruptedException {
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        final int numNodes = 20;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final List<Cluster> serviceList = new ArrayList<>();

        final Cluster seed = Cluster.start(seedHost);
        serviceList.add(seed);
        try {
            for (int i = 0; i < numNodes; i++) {
                final HostAndPort joiningHost = HostAndPort.fromParts("127.0.0.1", 1235 + i);
                final Cluster nonSeed = Cluster.join(seedHost, joiningHost);
                serviceList.add(nonSeed);
                assertEquals(i + 2, nonSeed.getMemberlist().size());
            }

            Thread.sleep(100);
            for (final Cluster cluster: serviceList) {
                assertEquals(cluster.getMemberlist().size(), numNodes + 1); // +1 for the seed
            }
        }
        catch (final Exception e) {
            e.printStackTrace();
            fail();
        }
        finally {
            for (final Cluster service: serviceList) {
                service.shutdown();
            }
        }
    }


    /**
     * Identical to the previous test, but with more than K nodes joining in parallel.
     *
     * The test starts with a single seed and all N - 1 subsequent nodes initiate their join protocol at the same
     * time. This tests a single seed's ability to bootstrap a large cluster in one step.
     */
    @Test
    public void testJoinMoreThanKSingleStepParallel() throws IOException, InterruptedException {
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        // Use a high interval because we are running on a single machine
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 5000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 10000;

        final int numNodes = 500;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final LinkedBlockingQueue<Cluster> serviceList = new LinkedBlockingQueue<>();

        final Cluster seed = Cluster.start(seedHost);
        serviceList.add(seed);
        final Executor executor = Executors.newWorkStealingPool(numNodes);
        try {
            final AtomicInteger nodeCounter = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(numNodes);

            for (int i = 0; i < numNodes; i++) {
                executor.execute(() -> {
                    try {
                        final HostAndPort joiningHost =
                                HostAndPort.fromParts("127.0.0.1", 1235 + nodeCounter.incrementAndGet());
                        final Cluster nonSeed = Cluster.join(seedHost, joiningHost);
                        serviceList.add(nonSeed);
                    } catch (final IOException | InterruptedException e) {
                        fail();
                    }
                    finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            for (final Cluster cluster: serviceList) {
                assertEquals(cluster.getMemberlist().size(), numNodes + 1); // +1 for the seed
                assertEquals(cluster.getMemberlist(), seed.getMemberlist());
            }
        }
        catch (final Exception e) {
            e.printStackTrace();
            fail();
        }
        finally {
            for (final Cluster service: serviceList) {
                service.shutdown();
            }
        }
    }


    /**
     * This test starts with a single seed, and a wave where 50 subsequent nodes initiate their join protocol
     * concurrently. Following this, a subsequent wave begins where 500 nodes then start together.
     */
    @Test
    public void testJoinMoreThanKParallelTwoWaves() throws IOException, InterruptedException {
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        // Use a high interval because we are running on a single machine
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 10000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 10000;

        final int numNodesPhase1 = 50;
        final int numNodesPhase2 = 500;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final LinkedBlockingQueue<Cluster> serviceList = new LinkedBlockingQueue<>();

        final Cluster seed = Cluster.start(seedHost);
        serviceList.add(seed);
        final Executor executor = Executors.newWorkStealingPool(numNodesPhase2);
        try {
            // Phase 1 where numNodesPhase1 entities join the network
            {
                final AtomicInteger nodeCounter = new AtomicInteger(0);
                final CountDownLatch latch = new CountDownLatch(numNodesPhase1);
                for (int i = 0; i < numNodesPhase1; i++) {
                    executor.execute(() -> {
                        try {
                            final HostAndPort joiningHost =
                                    HostAndPort.fromParts("127.0.0.1", 1235 + nodeCounter.incrementAndGet());
                            final Cluster nonSeed = Cluster.join(seedHost, joiningHost);
                            serviceList.add(nonSeed);
                        } catch (final IOException | InterruptedException e) {
                            fail();
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                latch.await();
                for (final Cluster cluster : serviceList) {
                    assertEquals(cluster.getMemberlist().size(), numNodesPhase1 + 1); // +1 for the seed
                    assertEquals(cluster.getMemberlist(), seed.getMemberlist());
                }
            }
            // Phase 2 where numNodesPhase2 entities join the network
            {
                final AtomicInteger nodeCounter = new AtomicInteger(0);
                final CountDownLatch latch = new CountDownLatch(numNodesPhase2);
                for (int i = 0; i < numNodesPhase2; i++) {
                    executor.execute(() -> {
                        try {
                            final HostAndPort joiningHost =
                                    HostAndPort.fromParts("127.0.0.1", 1235 + numNodesPhase1 +
                                            nodeCounter.incrementAndGet());
                            final Cluster nonSeed = Cluster.join(seedHost, joiningHost);
                            serviceList.add(nonSeed);
                        } catch (final IOException | InterruptedException e) {
                            fail();
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                latch.await();
                for (final Cluster cluster : serviceList) {
                    assertEquals(cluster.getMemberlist().size(), numNodesPhase1
                                                                        + numNodesPhase2 + 1); // +1 for the seed
                    assertEquals(cluster.getMemberlist(), seed.getMemberlist());
                }
            }
        }
        catch (final Exception e) {
            e.printStackTrace();
            fail();
        }
        finally {
            for (final Cluster service: serviceList) {
                service.shutdown();
            }
        }
    }

    /**
     * This test starts with a 3 node cluster. We then fail a single node to see if the monitoring mechanism
     * identifies the offending node.
     */
    @Test
    public void testNodeFailureAndMonitoring() throws IOException {
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        // Set a low probing interval.
        MembershipService.FAILURE_DETECTOR_INITIAL_DELAY_IN_MS = 1000;
        MembershipService.FAILURE_DETECTOR_INTERVAL_IN_MS = 100;

        final int numNodes = 2;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final ConcurrentHashMap<HostAndPort, Cluster> clusterInstances = new ConcurrentHashMap<>();

        final Cluster seed = Cluster.start(seedHost);
        clusterInstances.put(seedHost, seed);
        final Executor executor = Executors.newWorkStealingPool(numNodes);
        try {
            final AtomicInteger nodeCounter = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(numNodes);

            for (int i = 0; i < numNodes; i++) {
                executor.execute(() -> {
                    try {
                        final HostAndPort joiningHost =
                                HostAndPort.fromParts("127.0.0.1", 1235 + nodeCounter.incrementAndGet());
                        final Cluster nonSeed = Cluster.join(seedHost, joiningHost);
                        clusterInstances.put(joiningHost, nonSeed);
                    } catch (final IOException | InterruptedException e) {
                        fail();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            for (final Cluster cluster : clusterInstances.values()) {
                assertEquals(cluster.getMemberlist().size(), numNodes + 1); // +1 for the seed
                assertEquals(cluster.getMemberlist(), seed.getMemberlist());
            }

            // By this point, we have a twenty node cluster. Now let's shutdown a single host.
            final HostAndPort crashingHost = HostAndPort.fromParts("127.0.0.1", 1236);
            clusterInstances.get(crashingHost).shutdown();
            clusterInstances.remove(crashingHost);

            Thread.sleep(4000);
            for (final Cluster cluster : clusterInstances.values()) {
                assertEquals(cluster.getMemberlist().size(), numNodes); //
                assertEquals(cluster.getMemberlist(), seed.getMemberlist());
            }
        }
        catch (final Exception e) {
            e.printStackTrace();
            fail();
        }
        finally {
            for (final Cluster service: clusterInstances.values()) {
                service.shutdown();
            }
        }
    }
}