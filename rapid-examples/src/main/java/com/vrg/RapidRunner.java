package com.vrg;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.Cluster;
import com.vrg.rapid.NodeStatusChange;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by lsuresh on 5/25/17.
 */
class RapidRunner {
    @Nullable private final static Logger nettyLogger;
    @Nullable private final static Logger grpcLogger;
    private final Cluster cluster;
    private final HostAndPort listenAddress;

    static {
        grpcLogger = Logger.getLogger("io.grpc");
        grpcLogger.setLevel(Level.WARNING);
        nettyLogger = Logger.getLogger("io.grpc.netty.NettyServerHandler");
        nettyLogger.setLevel(Level.OFF);
    }

    RapidRunner(final HostAndPort listenAddress, final HostAndPort seedAddress,
                       final int sleepDelayMsForNonSeed)
            throws IOException, InterruptedException {
        this.listenAddress = listenAddress;
        if (listenAddress.equals(seedAddress)) {
            cluster = new Cluster.Builder(listenAddress).start();

        } else {
            Thread.sleep(sleepDelayMsForNonSeed);
            cluster = new Cluster.Builder(listenAddress).join(seedAddress);
        }
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE_PROPOSAL event occurs.
     */
    private void onViewChangeProposal(final List<NodeStatusChange> viewChange) {
        System.out.println("The condition detector has outputted a proposal: " + viewChange);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE_ONE_STEP_FAILED event occurs.
     */
    private void onViewChangeOneStepFailed(final List<NodeStatusChange> viewChange) {
        System.out.println("The condition detector had a conflict during one-step consensus: " + viewChange);
    }

    /**
     * Executed whenever a Cluster KICKED event occurs.
     */
    private void onKicked(final List<NodeStatusChange> viewChange) {
        System.out.println("We got kicked from the network: " + viewChange);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE event occurs.
     */
    private void onViewChange(final List<NodeStatusChange> viewChange) {
        System.out.println("View change detected: " + viewChange);
    }

    /**
     * Wait inside a loop
     */
    void run(final int maxTries, final int sleepIntervalMs) throws InterruptedException {
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE_PROPOSAL,
                this::onViewChangeProposal);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE,
                this::onViewChange);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE_ONE_STEP_FAILED,
                this::onViewChangeOneStepFailed);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.KICKED,
                this::onKicked);

        int tries = maxTries;
        while (tries-- > 0) {
            System.out.println(System.currentTimeMillis() + " " + listenAddress +
                    " Cluster size " + cluster.getMemberlist().size() + " " + tries);
            Thread.sleep(sleepIntervalMs);
        }
    }
}
