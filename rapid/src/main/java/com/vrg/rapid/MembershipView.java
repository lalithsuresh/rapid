/*
 * Copyright © 2016 - 2017 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.vrg.rapid;

import com.google.common.collect.ImmutableList;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.NodeId;
import net.openhft.hashing.LongHashFunction;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Hosts K permutations of the memberlist that represent the monitoring
 * relationship between nodes; every node monitors its successor on each ring.
 *
 */
@ThreadSafe
final class MembershipView {
    private final int K;
    private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx(0);
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    @GuardedBy("rwLock") private final Map<Integer, NavigableSet<Endpoint>> rings;
    @GuardedBy("rwLock") private final Set<NodeId> identifiersSeen = new TreeSet<>(NodeIdComparator.INSTANCE);
    @GuardedBy("rwLock") private long currentConfigurationId = -1;
    @GuardedBy("rwLock") private Configuration currentConfiguration;
    @GuardedBy("rwLock") private boolean shouldUpdateConfigurationId = true;

    MembershipView(final int K) {
        assert K > 0;
        this.K = K;
        this.rings = new HashMap<>(K);
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new TreeSet<>(Utils.AddressComparator.getComparatorWithSeed(k)));
        }
        this.currentConfiguration = new Configuration(identifiersSeen, rings.get(0));
    }

    /**
     * Used to bootstrap a membership view from the fields of a MembershipView.Settings object.
     */
    MembershipView(final int K, final Collection<NodeId> nodeIds,
                   final Collection<Endpoint> endpoints) {
        assert K > 0;
        this.K = K;
        this.rings = new HashMap<>(K);
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new TreeSet<>(Utils.AddressComparator.getComparatorWithSeed(k)));
            this.rings.get(k).addAll(endpoints);
        }
        this.identifiersSeen.addAll(nodeIds);
        this.currentConfiguration = new Configuration(identifiersSeen, rings.get(0));
    }

    /**
     * Queries if a host with a logical identifier {@code uuid} is safe to add to the network.
     *
     * @param node the joining node
     * @param uuid the joining node's identifier.
     * @return HOSTNAME_ALREADY_IN_RING if the {@code node} is already in the ring.
     *         UUID_ALREADY_IN_RING if the {@code uuid} is already seen before.
     *         SAFE_TO_JOIN otherwise.
     */
    JoinStatusCode isSafeToJoin(final Endpoint node, final NodeId uuid) {
        rwLock.readLock().lock();
        try {
            if (rings.get(0).contains(node)) {
                return JoinStatusCode.HOSTNAME_ALREADY_IN_RING;
            }

            if (identifiersSeen.contains(uuid)) {
                return JoinStatusCode.UUID_ALREADY_IN_RING;
            }

            return JoinStatusCode.SAFE_TO_JOIN;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Add a node to all K rings and records its unique identifier
     *
     * @param node the node to be added
     * @param nodeId the logical identifier of the node being added
     */
    void ringAdd(final Endpoint node, final NodeId nodeId) {
        Objects.requireNonNull(node);
        Objects.requireNonNull(nodeId);

        if (isIdentifierPresent(nodeId)) {
            throw new UUIDAlreadySeenException(node, nodeId);
        }

        rwLock.writeLock().lock();
        try {
            if (rings.get(0).contains(node)) {
                throw new NodeAlreadyInRingException(node);
            }

            for (int k = 0; k < K; k++) {
                rings.get(k).add(node);
            }

            identifiersSeen.add(nodeId);
            shouldUpdateConfigurationId = true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Delete a host from all K rings.
     *
     * @param node the host to be removed
     */
    void ringDelete(final Endpoint node) {
        Objects.requireNonNull(node);
        rwLock.writeLock().lock();
        try {

            if (!rings.get(0).contains(node)) {
                throw new NodeNotInRingException(node);
            }

            for (int k = 0; k < K; k++) {
                rings.get(k).remove(node);
            }

            shouldUpdateConfigurationId = true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Returns the set of monitors for {@code node}

     * @param node input node
     * @return the set of monitors for {@code node}
     * @throws NodeNotInRingException thrown if {@code node} is not in the ring
     */
    List<Endpoint> getMonitorsOf(final Endpoint node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            if (!rings.get(0).contains(node)) {
                throw new NodeNotInRingException(node);
            }

            if (rings.get(0).size() <= 1) {
                return Collections.emptyList();
            }

            final List<Endpoint> monitors = new ArrayList<>();

            for (int k = 0; k < K; k++) {
                final NavigableSet<Endpoint> list = rings.get(k);
                final Endpoint successor = list.higher(node);
                if (successor == null) {
                    monitors.add(list.first());
                }
                else {
                    monitors.add(successor);
                }
            }
            return monitors;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns the set of nodes monitored by {@code node}

     * @param node input node
     * @return the set of nodes monitored by {@code node}
     * @throws NodeNotInRingException thrown if {@code node} is not in the ring
     */
    List<Endpoint> getMonitoreesOf(final Endpoint node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            if (!rings.get(0).contains(node)) {
                throw new NodeNotInRingException(node);
            }

            if (rings.get(0).size() <= 1) {
                return Collections.emptyList();
            }
            return getPredecessorsOf(node);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Returns the expected monitors of {@code node}, even before it is
     * added to the ring. Used during the bootstrap protocol to identify
     * the nodes responsible for gatekeeping a joining peer.
     *
     * @param node input node
     * @return the list of nodes monitored by {@code node}. Empty list if the membership is empty.
     */
    List<Endpoint> getExpectedMonitorsOf(final Endpoint node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            if (rings.get(0).isEmpty()) {
                return Collections.emptyList();
            }
            return getPredecessorsOf(node);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Used by getExpectedMonitorsOf() and getMonitorsOf().
     */
    private List<Endpoint> getPredecessorsOf(final Endpoint node) {
        final List<Endpoint> monitorees = new ArrayList<>();

        for (int k = 0; k < K; k++) {
            final NavigableSet<Endpoint> list = rings.get(k);
            final Endpoint predecessor = list.lower(node);
            if (predecessor == null) {
                monitorees.add(list.last());
            }
            else {
                monitorees.add(predecessor);
            }
        }
        return monitorees;
    }

    /**
     * Query if a host is part of the current membership set.
     *
     * @param address the host
     * @return True if the node is present in the membership view and false otherwise.
     */
    boolean isHostPresent(final Endpoint address) {
        rwLock.readLock().lock();
        try {
            return rings.get(0).contains(address);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Query if an identifier has been used by a node already.
     *
     * @param identifier the identifier to query for
     * @return True if the identifier has been seen before and false otherwise.
     */
    boolean isIdentifierPresent(final NodeId identifier) {
        rwLock.readLock().lock();
        try {
            return identifiersSeen.contains(identifier);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Get the current identifier of the configuration. Computed based on the
     * set of nodes in the view as well as the identifiers seen so far.
     *
     * @return the current configuration identifier.
     */
    long getCurrentConfigurationId() {
        rwLock.readLock().lock();
        try {
            if (shouldUpdateConfigurationId) {
                updateCurrentConfigurationId();
                shouldUpdateConfigurationId = false;
            }
            return currentConfigurationId;
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Get the list of endpoints in the k'th ring.
     *
     * @param k the index of the ring to query
     * @return the list of endpoints in the k'th ring.
     */
    List<Endpoint> getRing(final int k) {
        rwLock.readLock().lock();
        try {
            assert k >= 0;
            return ImmutableList.copyOf(rings.get(k));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Get the ring number of a monitor for a given monitoree
     *
     * @param monitor The monitor node
     * @param monitoree The monitoree node
     * @return the indexes k such that {@code monitoree} is a successor of {@code monitoree} on ring[k].
     */
    List<Integer> getRingNumbers(final Endpoint monitor, final Endpoint monitoree) {
        rwLock.readLock().lock();
        try {
            // TODO: do this in one scan
            final List<Endpoint> monitorees = getMonitoreesOf(monitor);
            if (monitorees.isEmpty()) {
                return Collections.emptyList();
            }

            final List<Integer> ringIndexes = new ArrayList<>();
            int ringNumber = 0;
            for (final Endpoint node: monitorees) {
                if (node.equals(monitoree)) {
                    ringIndexes.add(ringNumber);
                }
                ringNumber++;
            }
            return ringIndexes;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Get the number of nodes currently in the membership.
     *
     * @return the number of nodes in the membership.
     */
    int getMembershipSize() {
        rwLock.readLock().lock();
        try {
            return rings.get(0).size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * XXX: May not be stable across processes. Verify.
     */
    @GuardedBy("rwLock")
    private void updateCurrentConfigurationId() {
        currentConfiguration = new Configuration(identifiersSeen, rings.get(0));
        currentConfigurationId = currentConfiguration.getConfigurationId();
    }

    /**
     * Get a Settings object that contains the list of nodes in the membership view
     * as well as the identifiers seen so far. These two lists suffice to bootstrap an
     * identical copy of the MembershipView object.
     *
     * @return a {@code Settings} object.
     */
    Configuration getConfiguration() {
        rwLock.readLock().lock();
        try {
            if (shouldUpdateConfigurationId) {
                updateCurrentConfigurationId();
                shouldUpdateConfigurationId = false;
            }
            return currentConfiguration;
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    private static final class NodeIdComparator implements Comparator<NodeId>, Serializable {
        private static final long serialVersionUID = -4891729395L;
        private static final NodeIdComparator INSTANCE = new NodeIdComparator();

        private NodeIdComparator() {
        }

        @Override
        public int compare(final NodeId o1, final NodeId o2) {
            // First, compare high bits
            if (o1.getHigh() < o2.getHigh()) {
                return -1;
            }
            if (o1.getHigh() > o2.getHigh()) {
                return 1;
            }
            // High bits are equal, so compare low bits
            if (o1.getLow() < o2.getLow()) {
                return -1;
            }
            if (o1.getLow() > o2.getLow()) {
                return 1;
            }
            // High and low bits are equal
            return 0;
        }
    }

    static class NodeAlreadyInRingException extends RuntimeException {
        NodeAlreadyInRingException(final Endpoint node) {
            super(node.toString());
        }
    }

    static class NodeNotInRingException extends RuntimeException {
        NodeNotInRingException(final Endpoint node) {
            super(node.toString());
        }
    }

    static class UUIDAlreadySeenException extends RuntimeException {
        UUIDAlreadySeenException(final Endpoint node, final NodeId nodeId) {
            super("Endpoint add attempt with identifier already seen:" +
                    " {host: " + node + ", identifier: " + nodeId + "}:");
        }
    }

    /**
     * The Settings object contains a list of nodes in the membership view as well as a list of UUIDs.
     * An instance of this object created from one MembershipView object contains the necessary information
     * to bootstrap an identical MembershipView object.
     */
    static class Configuration {
        final List<NodeId> nodeIds;
        final List<Endpoint> endpoints;

        public Configuration(final Set<NodeId> nodeIds, final Set<Endpoint> endpoints) {
            this.nodeIds = ImmutableList.copyOf(nodeIds);
            this.endpoints = ImmutableList.copyOf(endpoints);
        }

        /**
         * Gets the configuration ID for the list of endpoints and identifiers.
         *
         * @return a configuration identifier.
         */
        public long getConfigurationId() {
            return getConfigurationId(this.nodeIds, this.endpoints);
        }

        static long getConfigurationId(final Collection<NodeId> identifiers,
                                       final Collection<Endpoint> endpoints) {
            long hash = 1;
            for (final NodeId id: identifiers) {
                hash = hash * 37 + HASH_FUNCTION.hashLong(id.getHigh());
                hash = hash * 37 + HASH_FUNCTION.hashLong(id.getLow());
            }
            for (final Endpoint endpoint : endpoints) {
                hash = hash * 37 + HASH_FUNCTION.hashChars(endpoint.getHostname());
                hash = hash * 37 + HASH_FUNCTION.hashInt(endpoint.getPort());
            }
            return hash;
        }

    }
}