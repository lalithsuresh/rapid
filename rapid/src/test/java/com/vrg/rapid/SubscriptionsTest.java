package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

/**
 * Tests whether subscription callbacks are invoked on cluster starts/joins
 */
public class SubscriptionsTest {
    @Test(timeout = 5000)
    public void testSubscriptionOnJoin() throws IOException, InterruptedException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final HostAndPort joiner = HostAndPort.fromParts("127.0.0.1", 1235);

        // Initialize seed
        final TestCallback seedCb = new TestCallback();
        final Cluster seedCluster = new Cluster.Builder(seedHost)
                                               .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb)
                                               .start();

        // Initialize joiner
        final TestCallback joinCb = new TestCallback();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinCb)
                .join(seedHost);

        assertEquals(2, seedCb.numTimesCalled());
        assertEquals(1, joinCb.numTimesCalled());

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    @Test(timeout = 5000)
    public void testMultipleSubscriptionsOnJoin() throws IOException, InterruptedException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final HostAndPort joiner = HostAndPort.fromParts("127.0.0.1", 1235);

        // Initialize seed
        final TestCallback seedCb1 = new TestCallback();
        final TestCallback seedCb2 = new TestCallback();
        final Cluster seedCluster = new Cluster.Builder(seedHost)
                .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb1)
                .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb2)
                .start();

        // Initialize joiner
        final TestCallback joinCb1 = new TestCallback();
        final TestCallback joinCb2 = new TestCallback();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinCb1)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinCb2)
                .join(seedHost);

        assertEquals(2, seedCb1.numTimesCalled());
        assertEquals(2, seedCb2.numTimesCalled());
        assertEquals(1, joinCb1.numTimesCalled());
        assertEquals(1, joinCb2.numTimesCalled());

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    @Test(timeout = 5000)
    public void testSubscriptionPostJoin() throws IOException, InterruptedException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final HostAndPort joiner = HostAndPort.fromParts("127.0.0.1", 1235);

        // Initialize seed
        final TestCallback seedCb1 = new TestCallback();
        final Cluster seedCluster = new Cluster.Builder(seedHost)
                .addSubscription(ClusterEvents.VIEW_CHANGE, seedCb1)
                .start();

        final TestCallback seedCb2 = new TestCallback();
        seedCluster.registerSubscription(ClusterEvents.VIEW_CHANGE, seedCb2);

        // Initialize joiner
        final TestCallback consumer1 = new TestCallback();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, consumer1)
                .join(seedHost);

        assertEquals(2, seedCb1.numTimesCalled());
        assertEquals(1, seedCb2.numTimesCalled());
        assertEquals(1, consumer1.numTimesCalled());

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    private static class TestCallback implements Consumer<List<NodeStatusChange>> {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void accept(final List<NodeStatusChange> nodeStatusChanges) {
            counter.incrementAndGet();
        }

        int numTimesCalled() {
            return counter.get();
        }
    }
}
