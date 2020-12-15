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

package com.vrg.standalone;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.Cluster;
import com.vrg.rapid.ClusterStatusChange;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

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
        if (listenAddress.equals(seedAddress)) {
            cluster = new Cluster.Builder(listenAddress)
                    .start();

        } else {
            cluster = new Cluster.Builder(listenAddress)
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
    void onViewChangeProposal(final ClusterStatusChange viewChange) {
        LOG.info("The condition detector has outputted a proposal: {}", viewChange);
    }

    /**
     * Executed whenever a Cluster KICKED event occurs.
     */
    void onKicked(final ClusterStatusChange viewChange) {
        LOG.info("We got kicked from the network: {}", viewChange);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE event occurs.
     */
    void onViewChange(final ClusterStatusChange viewChange) {
        LOG.info("View change detected: {}", viewChange);
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