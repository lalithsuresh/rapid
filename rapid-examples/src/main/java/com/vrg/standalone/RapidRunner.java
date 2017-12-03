package com.vrg.standalone;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.Cluster;
import com.vrg.rapid.NodeStatusChange;
import com.vrg.rapid.Settings;
import com.vrg.rapid.SharedResources;
import com.vrg.rapid.messaging.impl.NettyDirectTcpClient;
import com.vrg.rapid.messaging.impl.NettyDirectTcpServer;
import com.vrg.rapid.pb.Endpoint;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Brings up Rapid Cluster instances.
 */
class RapidRunner {
    @Nullable private static final Logger NETTY_LOGGER;
    @Nullable private static final Logger GRPC_LOGGER;
    private final Cluster cluster;
    private final HostAndPort listenAddress;

    static {
        GRPC_LOGGER = Logger.getLogger("io.grpc");
        GRPC_LOGGER.setLevel(Level.WARNING);
        NETTY_LOGGER = Logger.getLogger("io.grpc.netty.NettyServerHandler");
        NETTY_LOGGER.setLevel(Level.OFF);
    }

    RapidRunner(final HostAndPort listenAddress, final HostAndPort seedAddress,
                final String role, final int sleepDelayMsForNonSeed)
            throws IOException, InterruptedException {
        this.listenAddress = listenAddress;
        final Settings settings = new Settings();
        settings.setGrpcJoinTimeoutMs(20000);
        final Endpoint endpoint = Endpoint.newBuilder().setHostname(listenAddress.getHost())
                                                       .setPort(listenAddress.getPort()).build();
        final SharedResources resources = new SharedResources(endpoint);
        final NettyDirectTcpClient tcpClient = new NettyDirectTcpClient(resources);
        final NettyDirectTcpServer tcpServer = new NettyDirectTcpServer(endpoint, resources);
        if (listenAddress.equals(seedAddress)) {
            cluster = new Cluster.Builder(listenAddress)
                                 .useSettings(settings)
                                 .setMessagingClientAndServer(tcpClient, tcpServer)
                                 .start();

        } else {
            Thread.sleep(sleepDelayMsForNonSeed);
            cluster = new Cluster.Builder(listenAddress)
                                 .useSettings(settings)
                                 .setMessagingClientAndServer(tcpClient, tcpServer)
                                 .join(seedAddress);
        }
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE_PROPOSAL,
                this::onViewChangeProposal);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE,
                this::onViewChange);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE_ONE_STEP_FAILED,
                this::onViewChangeOneStepFailed);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.KICKED,
                this::onKicked);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE_PROPOSAL event occurs.
     */
    private void onViewChangeProposal(final Long configurationId, final List<NodeStatusChange> viewChange) {
        System.out.println("The condition detector has outputted a proposal: " + viewChange + " " + configurationId);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE_ONE_STEP_FAILED event occurs.
     */
    private void onViewChangeOneStepFailed(final Long configurationId, final List<NodeStatusChange> viewChange) {
        System.out.println("The condition detector had a conflict during one-step consensus: "
                + viewChange + " " + configurationId);
    }

    /**
     * Executed whenever a Cluster KICKED event occurs.
     */
    private void onKicked(final Long configurationId, final List<NodeStatusChange> viewChange) {
        System.out.println("We got kicked from the network: " + viewChange + " " + configurationId);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE event occurs.
     */
    private void onViewChange(final Long configurationId, final List<NodeStatusChange> viewChange) {
        System.out.println("View change detected: " + viewChange + " " + configurationId);
    }

    /**
     * Wait inside a loop
     */
    void run(final int maxTries, final int sleepIntervalMs) throws InterruptedException {
        int tries = maxTries;
        while (tries-- > 0) {
            System.out.println(System.currentTimeMillis() + " " + listenAddress +
                    " Cluster size " + cluster.getMembershipSize() + " " + tries);
            Thread.sleep(sleepIntervalMs);
        }
    }

    String getClusterStatus() {
        return System.currentTimeMillis() + " " + listenAddress + " Cluster size " + cluster.getMembershipSize();
    }
}
