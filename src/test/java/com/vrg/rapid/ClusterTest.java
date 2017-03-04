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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

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
    public void testJoinUpToTen() throws IOException, InterruptedException {
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
     * Identical to the previous test, but with more than K nodes joining.
     */
    @Test
    public void testJoinMoreThanTen() throws IOException, InterruptedException {
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        final int numNodes = 300;
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
     * Identical to the previous test, but with more than K nodes joining.
     */
    @Test
    public void testJoinMoreThanTenParallel() throws IOException, InterruptedException {
        RpcServer.USE_IN_PROCESS_SERVER = true;
        RpcClient.USE_IN_PROCESS_CHANNEL = true;

        final int numNodes = 10;
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final LinkedBlockingQueue<Cluster> serviceList = new LinkedBlockingQueue<>();

        final Cluster seed = Cluster.start(seedHost);
        serviceList.add(seed);
        try {
            IntStream.range(0, numNodes).parallel().forEach( (int i) -> {
                try {
                    final HostAndPort joiningHost = HostAndPort.fromParts("127.0.0.1", 1235 + i);
                    final Cluster nonSeed = Cluster.join(seedHost, joiningHost);
                    serviceList.add(nonSeed);
                } catch (final IOException | InterruptedException e) {
                    fail();
                }
            });

            Thread.sleep(1000);
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
}