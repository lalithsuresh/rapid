package com.vrg;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.util.Timeout;
import com.google.common.net.HostAndPort;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.vrg.rapid.Cluster;
import com.vrg.rapid.ClusterEvents;
import com.vrg.rapid.NodeStatusChange;
import com.vrg.rapid.pb.LinkStatus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Rapid Cluster example.
 */
public class RapidStandalone
{
    @Nullable private static ActorSystem actorSystem;
    @Nullable private static ActorRef localActor;
    private static final String APPLICATION = "rapid-akka";
    private static final Timeout timeout = new Timeout(Duration.create(1000, "milliseconds"));

    /**
     * Executed whenever a Cluster VIEW_CHANGE_PROPOSAL event occurs.
     */
    private static void onViewChangeProposal(final List<NodeStatusChange> viewChange) {
        Objects.requireNonNull(localActor);
        Objects.requireNonNull(actorSystem);
        System.out.println("The condition detector has outputted a proposal: " + viewChange);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE_ONE_STEP_FAILED event occurs.
     */
    private static void onViewChangeOneStepFailed(final List<NodeStatusChange> viewChange) {
        Objects.requireNonNull(localActor);
        Objects.requireNonNull(actorSystem);
        System.out.println("The condition detector had a conflict during one-step consensus: " + viewChange);
    }

    /**
     * Executed whenever a Cluster KICKED event occurs.
     */
    private static void onKicked(final List<NodeStatusChange> viewChange) {
        Objects.requireNonNull(localActor);
        Objects.requireNonNull(actorSystem);
        System.out.println("We got kicked from the network: " + viewChange);
    }

    /**
     * Executed whenever a Cluster VIEW_CHANGE event occurs.
     */
    private static void onViewChange(final List<NodeStatusChange> viewChange) {
        Objects.requireNonNull(localActor);
        Objects.requireNonNull(actorSystem);

        final List<ActorRef> joinedNodes = viewChange.stream()
                                           .filter(e -> e.getStatus() == LinkStatus.UP)
                                           .map(RapidStandalone::getActorRefForHost)
                                           .collect(Collectors.toList());
        joinedNodes.forEach(actor -> actor.tell("Hello from " + localActor.toString(), localActor));
    }

    /**
     * Takes a node-change event and the associated metadata to obtain an ActorRef.
     */
    private static ActorRef getActorRefForHost(final NodeStatusChange statusChange) {
        Objects.requireNonNull(actorSystem);
        try {
            final String hostname = statusChange.getHostAndPort().getHost();    // Rapid host
            final String port = statusChange.getMetadata().get("akkaPort");     // Port for actor system
            return Await.result(actorSystem.actorSelection(
                "akka.tcp://" + APPLICATION + "@" + hostname + ":" + port + "/user/Printer").resolveOne(timeout),
                timeout.duration());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main( String[] args ) throws ParseException, IOException, InterruptedException {
        Logger.getLogger("io.grpc").setLevel(Level.WARNING);
        final Options options = new Options();
        options.addRequiredOption("l", "listenAddress", true, "The listening address for the Rapid Cluster");
        options.addRequiredOption("s", "seedAddress", true, "The seed node's address for the bootstrap protocol");
        options.addRequiredOption("a", "akkaAddress", true, "The akka system's address");
        options.addRequiredOption("r", "role", true, "The node's role for the cluster");
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);

        // Get CLI options
        final HostAndPort listenAddress = HostAndPort.fromString(cmd.getOptionValue("listenAddress"));
        final HostAndPort seedAddress = HostAndPort.fromString(cmd.getOptionValue("seedAddress"));
        final HostAndPort akkaAddress = HostAndPort.fromString(cmd.getOptionValue("akkaAddress"));
        final String role = cmd.getOptionValue("role");
        final int akkaPort = akkaAddress.getPort();

        // Initialize Actor system
        final Config config = ConfigFactory.parseString(
                "akka {\n" +
                        " stdout-loglevel = \"OFF\"\n" +
                        " loglevel = \"OFF\"\n" +
                        " actor {\n" +
                        "   provider = remote\n" +
                        " }\n" +
                        " serialization-bindings {\n" +
                        "   \"java.io.Serializable\" = none\n" +
                        " }\n" +
                        " remote {\n" +
                        "   enabled-transports = [\"akka.remote.netty.tcp\"]\n" +
                        "   netty.tcp {\n" +
                        "     hostname = \"" + akkaAddress.getHost() + "\"\n" +
                        "     port = " + akkaAddress.getPort() + "\n" +
                        "   }\n" +
                        " }\n" +
                        "}");
        actorSystem = ActorSystem.create(APPLICATION, config);
        assert actorSystem != null;
        localActor = actorSystem.actorOf(Props.create(Printer.class), "Printer");

        // Setup Rapid cluster
        final Map<String, String> metadata = new HashMap<>(2);
        metadata.put("role", role);
        metadata.put("akkaPort", String.valueOf(akkaPort));
        final Cluster cluster;
        if (listenAddress.equals(seedAddress)) {
            cluster = new Cluster.Builder(listenAddress)
                                 .setMetadata(metadata)
                                 .start();
        }
        else {
            cluster = new Cluster.Builder(listenAddress)
                                 .setMetadata(metadata)
                                 .join(seedAddress);
        }
        cluster.registerSubscription(ClusterEvents.VIEW_CHANGE_PROPOSAL, RapidStandalone::onViewChangeProposal);
        cluster.registerSubscription(ClusterEvents.VIEW_CHANGE, RapidStandalone::onViewChange);
        cluster.registerSubscription(ClusterEvents.VIEW_CHANGE_ONE_STEP_FAILED, RapidStandalone::onViewChangeOneStepFailed);
        cluster.registerSubscription(ClusterEvents.KICKED, RapidStandalone::onKicked);

        int rounds = 30;
        while (rounds-- > 0) {
            System.out.println(cluster.getMemberlist().size());
            Thread.sleep(5000);
        }
    }
}
