package com.vrg.standalone;

import com.google.common.net.HostAndPort;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Rapid Cluster example.
 */
public class StandaloneAgent {
    private static final int WAIT_DELAY_NON_SEED_MS = 10000;
    private static final int SLEEP_INTERVAL_MS = 1000;
    private static final int MAX_TRIES = 400;

    public static void main( String[] args ) throws ParseException, IOException, InterruptedException {
        final Options options = new Options();
        options.addRequiredOption("cluster", "cluster", true, "Cluster tool to use");
        options.addRequiredOption("l", "listenAddresses", true, "The listening addresses Rapid Cluster instances");
        options.addRequiredOption("s", "seedAddress", true, "The seed node's address for the bootstrap protocol");
        options.addRequiredOption("r", "role", true, "The node's role for the cluster");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        // Get CLI options
        final String clusterTool = cmd.getOptionValue("cluster");
        String addresses = cmd.getOptionValue("listenAddresses");
        addresses = addresses.replaceAll("\\s","");
        final List<HostAndPort> listenAddresses = Arrays.stream(addresses.split(",")).map(HostAndPort::fromString)
                                                      .collect(Collectors.toList());
        final HostAndPort seedAddress = HostAndPort.fromString(cmd.getOptionValue("seedAddress"));
        final String role = cmd.getOptionValue("role");
        final Executor executor = Executors.newWorkStealingPool(listenAddresses.size());

        if (clusterTool.equals("AkkaCluster")) {
            listenAddresses.forEach(listenAddress -> {
                final AkkaRunner runner = new AkkaRunner(listenAddress, seedAddress, WAIT_DELAY_NON_SEED_MS);
                executor.execute(() -> {
                    runner.run(MAX_TRIES, SLEEP_INTERVAL_MS);
                    System.exit(0);
                });
            });
        }
        else if (clusterTool.equals("Rapid")) {
            listenAddresses.forEach(listenAddress -> executor.execute(() -> {
                // Setup Rapid cluster and wait until completion
                try {
                    final RapidRunner runner = new RapidRunner(listenAddress, seedAddress, WAIT_DELAY_NON_SEED_MS);
                    runner.run(MAX_TRIES, SLEEP_INTERVAL_MS);
                } catch (final IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }));
        }
        Thread.currentThread().join();
    }
}