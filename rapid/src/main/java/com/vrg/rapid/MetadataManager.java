package com.vrg.rapid;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.grpc.ExperimentalApi;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-node metadata which is immutable. These are simple tags.
 *
 * For now, we only track roles. Depending on the requirements, we may extend this to
 * a list of key-value pairs per-node.
 */
@ExperimentalApi
final class MetadataManager {
    private Map<HostAndPort, List<String>> roleMap = new ConcurrentHashMap<>();

    /**
     * Get the list of roles for a node.
     *
     * @param node Node for which the roles are being set.
     */
    List<String> getRoles(final HostAndPort node) {
        Objects.requireNonNull(node);
        return roleMap.containsKey(node) ? ImmutableList.copyOf(roleMap.get(node)) : Collections.emptyList();
    }

    /**
     * Specify a set of roles for a node.
     *
     * @param node Node for which the roles are being set.
     * @param roles The list of roles for the node.
     */
    void setRoles(final HostAndPort node, final List<String> roles) {
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
