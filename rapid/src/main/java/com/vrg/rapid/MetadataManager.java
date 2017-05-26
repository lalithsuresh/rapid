package com.vrg.rapid;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.grpc.ExperimentalApi;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-node metadata which is immutable. These are simple tags like roles or other configuration parameters.
 */
@ExperimentalApi
final class MetadataManager {
    private final Map<HostAndPort, Map<String, String>> roleMap = new ConcurrentHashMap<>();

    /**
     * Get the list of roles for a node.
     *
     * @param node Node for which the roles are being set.
     */
    Map<String, String> get(final HostAndPort node) {
        Objects.requireNonNull(node);
        return roleMap.containsKey(node) ? ImmutableMap.copyOf(roleMap.get(node)) : Collections.emptyMap();
    }

    /**
     * Specify a set of roles for a node.
     *
     * @param node Node for which the roles are being set.
     * @param roles The list of roles for the node.
     */
    void setMetadata(final HostAndPort node, final Map<String, String> roles) {
        Objects.requireNonNull(node);
        Objects.requireNonNull(roles);
        roleMap.putIfAbsent(node, roles);
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
}
