package com.vrg;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AddressFromURIString;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.MemberStatus;
import akka.util.Timeout;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.vrg.rapid.NodeStatusChange;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Created by lsuresh on 5/25/17.
 */
class AkkaRunner {
    private final HostAndPort listenAddress;
    private static final String APPLICATION = "rapid-akka";
    private static final Timeout timeout = new Timeout(Duration.create(1000, "milliseconds"));
    private final ActorSystem actorSystem;
    private final Cluster cluster;

    AkkaRunner(final HostAndPort listenAddress, final HostAndPort seedAddress,
               final int sleepDelayMsForNonSeed) {
        this.listenAddress = listenAddress;
        final Config config = ConfigFactory.parseString(
                "akka {\n" +
                        " stdout-loglevel = \"OFF\"\n" +
                        " loglevel = \"OFF\"\n" +
                        " actor {\n" +
                        "   provider = akka.cluster.ClusterActorRefProvider\n" +
                        " }\n" +
                        " serialization-bindings {\n" +
                        "   \"java.io.Serializable\" = none\n" +
                        " }\n" +
                        " remote {\n" +
                        "   enabled-transports = [\"akka.remote.netty.tcp\"]\n" +
                        "   netty.tcp {\n" +
                        "     hostname = \"" + listenAddress.getHost() + "\"\n" +
                        "     port = " + listenAddress.getPort() + "\n" +
                        "   }\n" +
                        " }\n" +
                        "}");
        actorSystem = ActorSystem.create(APPLICATION, config);
        assert actorSystem != null;
        final ActorRef localActor = actorSystem.actorOf(Props.create(AkkaListener.class), "Actor:" + listenAddress);
        cluster = akka.cluster.Cluster.get(actorSystem);
        cluster.subscribe(localActor, akka.cluster.ClusterEvent.ClusterDomainEvent.class);
        cluster.join(AddressFromURIString.parse(seedUri(seedAddress)));
    }

    /**
     * Wait inside a loop
     */
    void run(final int maxTries, final int sleepIntervalMs) {
        int tries = maxTries;
        while (tries-- > 0) {
            final int up = ImmutableList.copyOf(cluster.state().getMembers())
                    .stream().filter(member -> member.status().equals(MemberStatus.up()))
                    .collect(Collectors.toList()).size();
            final int unreachable = cluster.state().getUnreachable().size();
            System.out.println(System.currentTimeMillis() + " " + listenAddress +
                    " Cluster size " + (up - unreachable) + " " + tries);
            try {
                Thread.sleep(sleepIntervalMs);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }


    /**
     * Takes a node-change event and the associated metadata to obtain an ActorRef.
     */
    private ActorRef getActorRefForHost(final NodeStatusChange statusChange) {
        Objects.requireNonNull(actorSystem);
        try {
            final String hostname = statusChange.getHostAndPort().getHost();    // Rapid host
            final String port = statusChange.getMetadata().get("akkaPort");     // Port for actor system
            return Await.result(actorSystem.actorSelection(
                    "akka.tcp://" + APPLICATION + "1@" + hostname + ":" + port + "/user/Printer").resolveOne(timeout),
                    timeout.duration());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String seedUri(final HostAndPort seedAddress) {
        return "akka.tcp://" + APPLICATION + "@" + seedAddress.getHost() + ":" + seedAddress.getPort();
    }
}
