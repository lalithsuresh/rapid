package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.TextFormat;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusResponse;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.Phase1aMessage;
import com.vrg.rapid.pb.Phase1bMessage;
import com.vrg.rapid.pb.Phase2aMessage;
import com.vrg.rapid.pb.Phase2bMessage;
import com.vrg.rapid.pb.PreJoinMessage;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import net.openhft.hashing.LongHashFunction;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Utility methods to convert protobuf types
 */
final class Utils {

    private Utils() {
    }

    /**
     * Helpers for type conversions
     */
    static NodeId nodeIdFromUUID(final UUID uuid) {
        return NodeId.newBuilder().setHigh(uuid.getMostSignificantBits())
                                  .setLow(uuid.getLeastSignificantBits()).build();
    }

    /**
     * Validate incoming host:port strings using Guava's HostAndPort
     */
    static Endpoint hostFromString(final String hostString) {
        final HostAndPort hostAndPort = HostAndPort.fromString(hostString);    // Validates input
        return Endpoint.newBuilder().setHostname(hostAndPort.getHost())
                                           .setPort(hostAndPort.getPort())
                                           .build();
    }

    /**
     * Validate incoming host:port strings using Guava's HostAndPort
     */
    static Endpoint hostFromParts(final String hostname, final int port) {
        final HostAndPort hostAndPort = HostAndPort.fromParts(hostname, port); // Validates input
        return Endpoint.newBuilder().setHostname(hostAndPort.getHost())
                .setPort(hostAndPort.getPort())
                .build();
    }

    /**
     * Protobuf messages have a toString() method that uses newlines, which does not bode
     * well with logging. This class allows a deferred toString() call on the protobuf object.
     */
    private static class Loggable<T extends GeneratedMessageV3> {
        private final T protobufObject;

        Loggable(final T protobufObject) {
            this.protobufObject = protobufObject;
        }

        @Override
        public String toString() {
            if (protobufObject instanceof Endpoint) {
                return ((Endpoint) protobufObject).getHostname() + ":" + ((Endpoint) protobufObject).getPort();
            }
            else {
                return TextFormat.shortDebugString(protobufObject);
            }
        }
    }

    /**
     * Wraps a protobuf object such that it has a logging friendly toString().
     * @param object protobuf object
     * @param <T> Any protobuf generated type
     * @return a Loggable instance which wraps the protobuf object.
     */
    static <T extends GeneratedMessageV3> Loggable loggable(final T object) {
        return new Loggable<>(object);
    }

    /**
     * Protobuf messages have a toString() method that uses newlines, which does not bode
     * well with logging. This class accepts a collection of such protobuf objects and returns
     * a toString implementation without newlines.
     */
    private static class LoggableCollection {
        private final Collection<? extends GeneratedMessageV3> protobufObjects;

        LoggableCollection(final Collection<? extends GeneratedMessageV3> protobufObjects) {
            this.protobufObjects = protobufObjects;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (final GeneratedMessageV3 obj: protobufObjects) {
                sb.append(loggable(obj));
                sb.append(", ");
            }
            sb.append("]");
            return sb.toString();
        }
    }

    /**
     * Wraps a collection of protobuf objects such that it has a logging friendly toString().
     * @param object collection of protobuf objects
     * @return a Loggable instance which wraps the protobuf collection.
     */
    static LoggableCollection loggable(final Collection<? extends GeneratedMessageV3> object) {
        return new LoggableCollection(object);
    }

    /**
     * Helpers to avoid the boilerplate of constructing a new RapidRequest/RapidResponse for
     * every message we want to send out.
     */
    static RapidRequest toRapidRequest(final PreJoinMessage msg) {
        return RapidRequest.newBuilder().setPreJoinMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final JoinMessage msg) {
        return RapidRequest.newBuilder().setJoinMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final BatchedLinkUpdateMessage msg) {
        return RapidRequest.newBuilder().setBatchedLinkUpdateMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final ProbeMessage msg) {
        return RapidRequest.newBuilder().setProbeMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final FastRoundPhase2bMessage msg) {
        return RapidRequest.newBuilder().setFastRoundPhase2BMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase1aMessage msg) {
        return RapidRequest.newBuilder().setPhase1AMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase1bMessage msg) {
        return RapidRequest.newBuilder().setPhase1BMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase2aMessage msg) {
        return RapidRequest.newBuilder().setPhase2AMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase2bMessage msg) {
        return RapidRequest.newBuilder().setPhase2BMessage(msg).build();
    }

    static RapidResponse toRapidResponse(final JoinResponse msg) {
        return RapidResponse.newBuilder().setJoinResponse(msg).build();
    }

    static RapidResponse toRapidResponse(final ConsensusResponse msg) {
        return RapidResponse.newBuilder().setConsensusResponse(msg).build();
    }

    static RapidResponse toRapidResponse(final ProbeResponse msg) {
        return RapidResponse.newBuilder().setProbeResponse(msg).build();
    }


    /**
     * Used to order endpoints in the different rings.
     */
    static final class AddressComparator implements Comparator<Endpoint>, Serializable {
        private static final long serialVersionUID = -4891729390L;
        private static final Map<Integer, AddressComparator> INSTANCES = new HashMap<>();
        private final LongHashFunction hashFunction;

        AddressComparator(final int seed) {
            this.hashFunction = LongHashFunction.xx(seed);
        }

        @Override
        public final int compare(final Endpoint c1, final Endpoint c2) {
            final long hash1 = hashFunction.hashChars(c1.getHostname()) * 31 + hashFunction.hashInt(c1.getPort());
            final long hash2 = hashFunction.hashChars(c2.getHostname()) * 31 + hashFunction.hashInt(c2.getPort());
            return Long.compare(hash1, hash2);
        }

        static synchronized AddressComparator getComparatorWithSeed(final int seed) {
            return INSTANCES.computeIfAbsent(seed, AddressComparator::new);
        }
    }
}