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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Standalone Rapid Cluster Daemon
 *
 */
public class RapidStandalone
{
    @Nullable private static ActorSystem actorSystem;
    @Nullable private static ActorRef localActor;
    private static final String APPLICATION = "rapid-akka";
    private static final Timeout timeout = new Timeout(Duration.create(1000, "milliseconds"));

    private static void onViewChange(final List<NodeStatusChange> viewChange) {
        Objects.requireNonNull(localActor);
        Objects.requireNonNull(actorSystem);

        final List<ActorRef> joinedNodes = viewChange.stream()
                                           .filter(e -> e.getStatus() == LinkStatus.UP)
                                           .map(NodeStatusChange::getHostAndPort)
                                           .map(host -> getActorRefForHost(host, "/user/Printer"))
                                           .collect(Collectors.toList());
        joinedNodes.forEach(actor -> actor.tell("Hello from " + localActor.toString(), localActor));
    }

    private static ActorRef getActorRefForHost(final HostAndPort host, final String path) {
        Objects.requireNonNull(actorSystem);
        final int akkaPort = host.getPort() + 1000;
        try {
            return Await.result(actorSystem.actorSelection(
                "akka.tcp://" + APPLICATION + "@" + host.getHost() + ":" + (akkaPort) + path).resolveOne(timeout),
                timeout.duration());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main( String[] args ) throws ParseException, IOException, InterruptedException {
        // create Options object
        final Options options = new Options();
        options.addRequiredOption("l", "listenAddress", true, "The listening address for the Rapid Cluster");
        options.addRequiredOption("s", "seedAddress", true, "The seed node's address for the bootstrap protocol");
        options.addRequiredOption("r", "role", true, "The node's role for the cluster");

        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args);
        Logger.getLogger("io.grpc").setLevel(Level.WARNING);

        final HostAndPort listenAddress = HostAndPort.fromString(cmd.getOptionValue("listenAddress"));
        final HostAndPort seedAddress = HostAndPort.fromString(cmd.getOptionValue("seedAddress"));
        final String role = cmd.getOptionValue("role");
        final Cluster cluster;
        final int akkaPort = (1000 + listenAddress.getPort()); // For now, akka port is 1000+ rapid port.
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
                        "     hostname = \"" + listenAddress.getHost() + "\"\n" +
                        "     port = " + akkaPort + "\n" +
                        "   }\n" +
                        " }\n" +
                        "}");

        actorSystem = ActorSystem.create(APPLICATION, config);
        assert actorSystem != null;
        localActor = actorSystem.actorOf(Props.create(Printer.class), "Printer");

        if (listenAddress.equals(seedAddress)) {
            // Start as a seed node
            cluster = Cluster.start(listenAddress, Collections.singletonList(role));
        }
        else {
            cluster = Cluster.join(seedAddress, listenAddress, Collections.singletonList(role));
        }

        cluster.registerSubscription(ClusterEvents.VIEW_CHANGE, RapidStandalone::onViewChange);

        while (true) {
            System.out.println(cluster.getMemberlist().size());
            Thread.sleep(5000);
        }
    }
}
