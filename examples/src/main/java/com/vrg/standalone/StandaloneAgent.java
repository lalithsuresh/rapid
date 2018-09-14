package com.vrg.standalone;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.Cluster;
import com.vrg.rapid.NodeStatusChange;
import com.vrg.rapid.Settings;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * Rapid Cluster example.
 */
public class StandaloneAgent {
    private static final Logger LOG = LoggerFactory.getLogger(StandaloneAgent.class);
    private static final int SLEEP_INTERVAL_MS = 1000;
    private static final int MAX_TRIES = 400;
    final HostAndPort listenAddress;
    final HostAndPort seedAddress;
    @Nullable private Cluster cluster = null;

    StandaloneAgent(final HostAndPort listenAddress, final HostAndPort seedAddress) {
        this.listenAddress = listenAddress;
        this.seedAddress = seedAddress;
    }

    public void startCluster() throws IOException, InterruptedException {
        // The first node X of the cluster calls .start(), the rest call .join(X)
        final Settings settings = new Settings();
        settings.setGrpcTimeoutMs(2000);
        settings.setGrpcProbeTimeoutMs(2000);
        settings.setGrpcJoinTimeoutMs(20000);
        if (listenAddress.equals(seedAddress)) {
            cluster = new Cluster.Builder(listenAddress)
                    .useSettings(settings)
                    .start();

        } else {
            cluster = new Cluster.Builder(listenAddress)
                    .useSettings(settings)
                    .join(seedAddress);
        }
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE_PROPOSAL,
                this::onViewChangeProposal);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.VIEW_CHANGE,
                this::onViewChange);
        cluster.registerSubscription(com.vrg.rapid.ClusterEvents.KICKED,
                this::onKicked);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE_PROPOSAL event occurs.
     */
    void onViewChangeProposal(final Long configurationId, final List<NodeStatusChange> viewChange) {
        LOG.info("The condition detector has outputted a proposal: {} {}", viewChange, configurationId);
    }

    /**
     * Executed whenever a Cluster KICKED event occurs.
     */
    void onKicked(final Long configurationId, final List<NodeStatusChange> viewChange) {
        LOG.info("We got kicked from the network: {} {}", viewChange, configurationId);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE event occurs.
     */
    void onViewChange(final Long configurationId, final List<NodeStatusChange> viewChange) {
        LOG.info("View change detected: {} {}", viewChange, configurationId);
    }

    /**
     * Prints the current membership
     */
    private void printClusterMembership() {
        LOG.info("Node {} -- cluster size {}", listenAddress, cluster.getMembershipSize());
    }

    public static void main(final String[] args) throws ParseException {
        final Options options = new Options();
        options.addRequiredOption("l", "listenAddress", true, "The listening addresses Rapid Cluster instances");
        options.addRequiredOption("s", "seedAddress", true, "The seed node's address for the bootstrap protocol");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        // Get CLI options
        final HostAndPort listenAddress = HostAndPort.fromString(cmd.getOptionValue("listenAddress"));
        final HostAndPort seedAddress = HostAndPort.fromString(cmd.getOptionValue("seedAddress"));

        // Bring up Rapid node
        try {
            final StandaloneAgent agent = new StandaloneAgent(listenAddress, seedAddress);
            agent.startCluster();
            for (int i = 0; i < MAX_TRIES; i++) {
                agent.printClusterMembership();
                Thread.sleep(SLEEP_INTERVAL_MS);
            }
        } catch (final IOException | InterruptedException e) {
            LOG.error("Exception thrown by StandaloneAgent {}", e);
            Thread.currentThread().interrupt();
        }
    }
}