package com.vrg.standalone;

import com.google.common.net.HostAndPort;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Rapid Cluster example.
 */
public class StandaloneAgent {
    private static final int WAIT_DELAY_NON_SEED_MS = 10000;
    private static final int SLEEP_INTERVAL_MS = 1000;
    private static final int MAX_TRIES = 400;

    public static void main(final String[] args) throws ParseException, IOException, InterruptedException {
        final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean) {
            final UnixOperatingSystemMXBean bean = (UnixOperatingSystemMXBean) os;
            System.out.println("File descriptor limit: " + bean.getMaxFileDescriptorCount());
        }
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
        final ExecutorService executor = Executors.newWorkStealingPool(listenAddresses.size());

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
            final Queue<RapidRunner> runners = new ConcurrentLinkedQueue<>();
            final CountDownLatch latch = new CountDownLatch(listenAddresses.size());
            listenAddresses.forEach(listenAddress -> executor.execute(() -> {
                // Setup Rapid cluster and wait until completion
                try {
                    final RapidRunner runner = new RapidRunner(listenAddress, seedAddress, role,
                                                               WAIT_DELAY_NON_SEED_MS);
                    runners.add(runner);
                } catch (final IOException | InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }));
            latch.await();
            executor.shutdownNow();
            int tries = MAX_TRIES;
            while (tries-- > 0) {
                for (final RapidRunner runner: runners) {
                    System.out.println(runner.getClusterStatus() + " " + tries);
                }
                Thread.sleep(SLEEP_INTERVAL_MS
                );
            }
        }
        Thread.currentThread().join();
    }
}