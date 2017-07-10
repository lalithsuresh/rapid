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
    @Test(timeout = 30000)
    public void testSubscriptionOnJoin() throws IOException, InterruptedException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final HostAndPort joiner = HostAndPort.fromParts("127.0.0.1", 1235);

        // Initialize seed
        final AtomicInteger seedCounter = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> consumerSeed = (List<NodeStatusChange> status)
                                                               -> seedCounter.incrementAndGet();
        final Cluster seedCluster = new Cluster.Builder(seedHost)
                                               .addSubscription(ClusterEvents.VIEW_CHANGE, consumerSeed)
                                               .start();

        // Initialize joiner
        final AtomicInteger joinerCounter = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> consumer = (List<NodeStatusChange> status)
                                                           -> joinerCounter.incrementAndGet();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, consumer)
                .join(seedHost);

        // Check if counters got incremented
        assertEquals(2, seedCounter.get());
        assertEquals(1, joinerCounter.get());

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    @Test(timeout = 30000)
    public void testMultipleSubscriptionsOnJoin() throws IOException, InterruptedException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final HostAndPort joiner = HostAndPort.fromParts("127.0.0.1", 1235);

        // Initialize seed
        final AtomicInteger seedCounter1 = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> consumerSeed1 = (List<NodeStatusChange> status)
                -> seedCounter1.incrementAndGet();
        final AtomicInteger seedCounter2 = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> consumerSeed2 = (List<NodeStatusChange> status)
                -> seedCounter2.incrementAndGet();
        final Cluster seedCluster = new Cluster.Builder(seedHost)
                .addSubscription(ClusterEvents.VIEW_CHANGE, consumerSeed1)
                .addSubscription(ClusterEvents.VIEW_CHANGE, consumerSeed2)
                .start();

        // Initialize joiner
        final AtomicInteger joinerCounter1 = new AtomicInteger(0);
        final AtomicInteger joinerCounter2 = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> joinerConsumer1 = (List<NodeStatusChange> status)
                -> joinerCounter1.incrementAndGet();
        final Consumer<List<NodeStatusChange>> joinerConsumer2 = (List<NodeStatusChange> status)
                -> joinerCounter2.incrementAndGet();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinerConsumer1)
                .addSubscription(ClusterEvents.VIEW_CHANGE, joinerConsumer2)
                .join(seedHost);

        // Check if counters got incremented
        assertEquals(2, seedCounter1.get());
        assertEquals(2, seedCounter2.get());
        assertEquals(1, joinerCounter1.get());
        assertEquals(1, joinerCounter2.get());

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

    @Test(timeout = 30000)
    public void testSubscriptionPostJoin() throws IOException, InterruptedException {
        final HostAndPort seedHost = HostAndPort.fromParts("127.0.0.1", 1234);
        final HostAndPort joiner = HostAndPort.fromParts("127.0.0.1", 1235);

        // Initialize seed
        final AtomicInteger seedCounter = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> consumerSeed = (List<NodeStatusChange> status)
                -> seedCounter.incrementAndGet();
        final Cluster seedCluster = new Cluster.Builder(seedHost)
                .addSubscription(ClusterEvents.VIEW_CHANGE, consumerSeed)
                .start();

        final AtomicInteger seedCounter2 = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> consumerSeed2 = (List<NodeStatusChange> status)
                -> seedCounter2.incrementAndGet();
        seedCluster.registerSubscription(ClusterEvents.VIEW_CHANGE, consumerSeed2);

        // Initialize joiner
        final AtomicInteger joinerCounter = new AtomicInteger(0);
        final Consumer<List<NodeStatusChange>> consumer = (List<NodeStatusChange> status)
                -> joinerCounter.incrementAndGet();
        final Cluster nonSeed = new Cluster.Builder(joiner)
                .addSubscription(ClusterEvents.VIEW_CHANGE, consumer)
                .join(seedHost);

        // Check if counters got incremented
        assertEquals(2, seedCounter.get());
        assertEquals(1, seedCounter2.get());
        assertEquals(1, joinerCounter.get());

        seedCluster.shutdown();
        nonSeed.shutdown();
    }

}
