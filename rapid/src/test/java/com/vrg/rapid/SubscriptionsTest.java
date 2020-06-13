/*
 * Copyright © 2016 - 2020 VMware, Inc. All Rights Reserved.
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

import com.vrg.rapid.pb.Endpoint;
import com.google.protobuf.ByteString;
import com.vrg.rapid.pb.EdgeStatus;
import com.vrg.rapid.pb.Metadata;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests whether subscription callbacks are invoked on cluster starts/joins
 */
public class SubscriptionsTest {
    /**
     * Two node cluster, one subscription each.
     */
    @Test(timeout = 5000)
    public void testSubscriptionOnJoin() throws IOException, InterruptedException {
        final Settings settings = new Settings();
        settings.setUseInProcessTransport(true);

        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", 1234);
        final Endpoint joiner = Utils.hostFromParts("127.0.0.1", 1235);

        // Initialize seed
        final TestCallback seedCb = new TestCallback();
        final Cluster seedCluster = new Cluster.Builder(seedEndpoint)
                                               .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb)
                                               .useSettings(settings)
                                               .start();

        // Initialize joiner
        final TestCallback joinCb = new TestCallback();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinCb)
                .useSettings(settings)
                .join(seedEndpoint);

        assertEquals(2, seedCb.numTimesCalled());
        assertEquals(1, joinCb.numTimesCalled());
        assertEquals(2, seedCb.getNotificationLog().size());
        assertEquals(1, joinCb.getNotificationLog().size());
        testNodeStatus(seedCb.getNotificationLog(), EdgeStatus.UP);
        testNodeStatus(joinCb.getNotificationLog(), EdgeStatus.UP);

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    /**
     * Two node cluster, two subscriptions each.
     */
    @Test(timeout = 5000)
    public void testMultipleSubscriptionsOnJoin() throws IOException, InterruptedException {
        final Settings settings = new Settings();
        settings.setUseInProcessTransport(true);

        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", 1234);
        final Endpoint joiner = Utils.hostFromParts("127.0.0.1", 1235);

        // Initialize seed
        final TestCallback seedCb1 = new TestCallback();
        final TestCallback seedCb2 = new TestCallback();
        final Cluster seedCluster = new Cluster.Builder(seedEndpoint)
                .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb1)
                .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb2)
                .useSettings(settings)
                .start();

        // Initialize joiner
        final TestCallback joinCb1 = new TestCallback();
        final TestCallback joinCb2 = new TestCallback();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinCb1)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinCb2)
                .useSettings(settings)
                .join(seedEndpoint);

        assertEquals(2, seedCb1.numTimesCalled());
        assertEquals(2, seedCb2.numTimesCalled());
        assertEquals(1, joinCb1.numTimesCalled());
        assertEquals(1, joinCb2.numTimesCalled());
        testNodeStatus(seedCb1.getNotificationLog(), EdgeStatus.UP);
        testNodeStatus(seedCb2.getNotificationLog(), EdgeStatus.UP);
        testNodeStatus(joinCb1.getNotificationLog(), EdgeStatus.UP);
        testNodeStatus(joinCb2.getNotificationLog(), EdgeStatus.UP);

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    /**
     * Two node cluster, seed adds a subscription after initialization.
     */
    @Test(timeout = 5000)
    public void testSubscriptionPostJoin() throws IOException, InterruptedException {
        final Settings settings = new Settings();
        settings.setUseInProcessTransport(true);

        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", 1234);
        final Endpoint joiner = Utils.hostFromParts("127.0.0.1", 1235);

        // Initialize seed
        final TestCallback seedCb1 = new TestCallback();
        final Cluster seedCluster = new Cluster.Builder(seedEndpoint)
                .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb1)
                .useSettings(settings)
                .start();

        final TestCallback seedCb2 = new TestCallback();
        seedCluster.registerSubscription(ClusterEvents.VIEW_CHANGE, seedCb2);

        // Initialize joiner
        final TestCallback joinCb1 = new TestCallback();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinCb1)
                .useSettings(settings)
                .join(seedEndpoint);

        assertEquals(2, seedCb1.numTimesCalled());
        assertEquals(1, seedCb2.numTimesCalled());
        assertEquals(1, joinCb1.numTimesCalled());
        testNodeStatus(seedCb1.getNotificationLog(), EdgeStatus.UP);
        testNodeStatus(seedCb2.getNotificationLog(), EdgeStatus.UP);
        testNodeStatus(joinCb1.getNotificationLog(), EdgeStatus.UP);

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    /**
     * Initialized 6 node cluster, then fail the seed node. Verify that the node that joined last
     * gets the notification about the failure, including the metadata about the seed node.
     */
    @Test(timeout = 10000)
    public void testSubscriptionWithFailure() throws IOException, InterruptedException {
        final Settings settings = new Settings();
        settings.setUseInProcessTransport(true);

        final List<StaticFailureDetector.Factory> fds = new ArrayList<>();
        final Endpoint seedEndpoint = Utils.hostFromParts("127.0.0.1", 1234);

        // Initialize seed
        final TestCallback seedCb1 = new TestCallback();
        final StaticFailureDetector.Factory fdFactory = new StaticFailureDetector.Factory(new HashSet<>());
        final ByteString byteString = ByteString.copyFrom("seed", Charset.defaultCharset());
        final Cluster seedCluster = new Cluster.Builder(seedEndpoint)
                .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb1)
                .setEdgeFailureDetectorFactory(fdFactory)
                .setMetadata(Collections.singletonMap("role", byteString))
                .useSettings(settings)
                .start();
        fds.add(fdFactory);

        // Initialize joiners
        final List<Cluster> joiners = new ArrayList<>();
        final List<TestCallback> callbacks = new ArrayList<>();
        final int numNodes = 5;
        for (int i = 0; i < numNodes; i++) {
            final Endpoint joiner = Utils.hostFromParts("127.0.0.1", 1235 + i);
            final StaticFailureDetector.Factory fdJoiner = new StaticFailureDetector.Factory(new HashSet<>());
            final TestCallback joinerCb1 = new TestCallback();
            joiners.add(new Cluster.Builder(joiner)
                    .addSubscription(ClusterEvents.VIEW_CHANGE, joinerCb1)
                    .setEdgeFailureDetectorFactory(fdJoiner)
                    .useSettings(settings)
                    .join(seedEndpoint));
            fds.add(fdJoiner);
            callbacks.add(joinerCb1);
        }

        // Each node will hear a number of notifications equal to the number of nodes that joined
        // after it, as well as the notification from its own initialization
        assertEquals(numNodes + 1, seedCb1.numTimesCalled());
        testNodeStatus(seedCb1.getNotificationLog(), EdgeStatus.UP);
        Thread.sleep(2000);
        for (int i = 0; i < numNodes; i++) {
            assertEquals(numNodes - i, callbacks.get(i).numTimesCalled());
            assertEquals(numNodes - i, callbacks.get(i).getNotificationLog().size());
            testNodeStatus(callbacks.get(i).getNotificationLog(), EdgeStatus.UP);
        }

        // Fail the seed node and wait for the dissemination to kick in
        seedCluster.shutdown();
        final Set<Endpoint> failedNodes = new HashSet<>();
        failedNodes.add(seedEndpoint);
        fds.forEach(e -> e.addFailedNodes(failedNodes));
        Thread.sleep(2000);

        // All joiners should receive one more event that includes the seed host having failed. This event
        // should be of type EdgeStatus.DOWN, and should also include the metadata about the seed node
        for (int i = 0; i < numNodes; i++) {
            assertEquals(numNodes - i + 1, callbacks.get(i).getNotificationLog().size());
            final List<NodeStatusChange> lastNotification = callbacks.get(i).getNotificationLog().get(numNodes - i);
            assertEquals(1, lastNotification.size());
            assertEquals(EdgeStatus.DOWN, lastNotification.get(0).getStatus());
            assertEquals(seedEndpoint, lastNotification.get(0).getEndpoint());

            // Now verify metadata
            final Metadata metadata = lastNotification.get(0).getMetadata();
            assertEquals(1, metadata.getMetadataCount());
            assertTrue(metadata.getMetadataMap().containsKey("role"));
            assertTrue(metadata.getMetadataMap().get("role").equals(ByteString.copyFrom("seed",
                                                                    Charset.defaultCharset())));
        }
        for (final Cluster cluster: joiners) {
            cluster.shutdown();
        }
    }

    /**
     * Helper that scans a notification log and checks whether all values match a given expectedValue.
     */
    private void testNodeStatus(final List<List<NodeStatusChange>> log, final EdgeStatus expectedValue) {
        for (final List<NodeStatusChange> entry: log) {
            for (final NodeStatusChange status: entry) {
                assertEquals(expectedValue, status.getStatus());
            }
        }
    }

    /**
     * Encapsulates a NodeStatusChange callback and counts the number of times it was invoked
     */
    private static class TestCallback implements BiConsumer<Long, List<NodeStatusChange>> {
        private final List<List<NodeStatusChange>> notificationLog = new ArrayList<>();

        @Override
        public void accept(final Long id, final List<NodeStatusChange> nodeStatusChanges) {
            notificationLog.add(nodeStatusChanges);
        }

        int numTimesCalled() {
            return notificationLog.size();
        }

        List<List<NodeStatusChange>> getNotificationLog() {
            return notificationLog;
        }
    }
}
