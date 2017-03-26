package com.vrg;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.Cluster;
import com.vrg.rapid.ClusterEvents;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standalone Rapid Cluster Daemon
 *
 */
public class RapidStandalone
{
    private static void onViewChange(final List<HostAndPort> viewChange) {
        System.out.println(viewChange);
    }


    public static void main( String[] args ) throws ParseException, IOException, InterruptedException {
        // create Options object
        final Options options = new Options();
        options.addRequiredOption("l", "listenAddress", true, "The listening address for the Rapid Cluster");
        options.addRequiredOption("s", "seedAddress", true, "The seed node's address for the bootstrap protocol");

        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);
        Logger.getLogger("io.grpc").setLevel(Level.WARNING);

        final HostAndPort listenAddress = HostAndPort.fromString(cmd.getOptionValue("listenAddress"));
        final HostAndPort seedAddress = HostAndPort.fromString(cmd.getOptionValue("seedAddress"));
        final Cluster cluster;

        if (listenAddress.equals(seedAddress)) {
            // Start as a seed node
            cluster = Cluster.start(listenAddress);
        }
        else {
            cluster = Cluster.join(seedAddress, listenAddress);
        }

        cluster.registerSubscription(ClusterEvents.VIEW_CHANGE, RapidStandalone::onViewChange);

        while (true) {
            System.out.println(cluster.getMemberlist().size());
            Thread.sleep(5000);
        }
    }
}
