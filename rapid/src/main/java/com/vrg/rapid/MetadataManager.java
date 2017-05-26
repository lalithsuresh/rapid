package com.vrg.rapid;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.Metadata;
import io.grpc.ExperimentalApi;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-node metadata which is immutable. These are simple tags like roles or other configuration parameters.
 */
@ExperimentalApi
@NotThreadSafe
final class MetadataManager {
    private final Map<HostAndPort, Metadata> roleMap = new ConcurrentHashMap<>();

    /**
     * Get the list of roles for a node.
     *
     * @param node Node for which the roles are being set.
     */
    Metadata get(final HostAndPort node) {
        Objects.requireNonNull(node);
        return roleMap.containsKey(node) ? roleMap.get(node) : Metadata.getDefaultInstance();
    }

    /**
     * Specify a set of roles for a node.
     *
     * @param roles The list of roles for the node.
     */
    void addMetadata(final Map<String, Metadata> roles) {
        Objects.requireNonNull(roles);
        roles.forEach((k, v) -> roleMap.putIfAbsent(HostAndPort.fromString(k), v));
    }

    /**
     * Remove a node from the role-set. Will throw NPE if called on a node that is not in the set.
     *
     * @param node Node for which the roles are being set.
     */
    void removeNode(final HostAndPort node) {
        Objects.requireNonNull(node);
        roleMap.remove(node);
    }

    /**
     * Get the list of all node tags. This is shared with joining nodes when they bootstrap.
     */
    Map<String, Metadata> getAllMetadata() {
        // XXX: Not happy with the back and forth conversion here. We should not require conversions
        // between HostAndPort and strings when crossing over from protobufs to rapid and vice-versa.
        final ImmutableMap.Builder<String, Metadata> stringMap = ImmutableMap.builder();
        roleMap.forEach((k, v) -> stringMap.put(k.toString(), v));
        return stringMap.build();
    }
}
