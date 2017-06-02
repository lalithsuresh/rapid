package com.vrg.standalone;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AddressFromURIString;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.MemberStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.stream.Collectors;


/**
 * Brings up an Akka Cluster instance
 */
class AkkaRunner {
    private final HostAndPort listenAddress;
    private static final String APPLICATION = "rapid-akka";
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

    private static String seedUri(final HostAndPort seedAddress) {
        return "akka.tcp://" + APPLICATION + "@" + seedAddress.getHost() + ":" + seedAddress.getPort();
    }
}
