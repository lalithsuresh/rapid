package com.vrg;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.Cluster;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

/**
 * Standalone Rapid Cluster Daemon
 *
 */
public class RapidStandalone
{
    public static void main( String[] args ) throws ParseException, IOException, InterruptedException {
        // create Options object
        final Options options = new Options();
        options.addRequiredOption("l", "listenAddress", true, "The listening address for the Rapid Cluster");
        options.addRequiredOption("s", "seedAddress", true, "The seed node's address for the bootstrap protocol");

        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        final HostAndPort listenAddress = HostAndPort.fromString(cmd.getOptionValue("listenAddress"));
        final HostAndPort seedAddress = HostAndPort.fromString(cmd.getOptionValue("seedAddress"));

        if (listenAddress.equals(seedAddress)) {
            // Start as a seed node
            Cluster.start(listenAddress);
        }
        else {
            Cluster.join(seedAddress, listenAddress);
        }

        Thread.currentThread().join();
    }
}
