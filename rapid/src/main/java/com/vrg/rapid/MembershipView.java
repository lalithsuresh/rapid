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
import com.google.common.net.HostAndPort;
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
 * TODO: too many scans of the k rings during reads. Maintain a cache.
 */
@ThreadSafe
final class MembershipView {
    private final int K;
    private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx(0);
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    @GuardedBy("rwLock") private final Map<Integer, NavigableSet<HostAndPort>> rings;
    @GuardedBy("rwLock") private final Set<NodeId> identifiersSeen = new TreeSet<>(NodeIdComparator.INSTANCE);
    @GuardedBy("rwLock") private long currentConfigurationId = -1;
    @GuardedBy("rwLock") private boolean shouldUpdateConfigurationId = true;

    MembershipView(final int K) {
        assert K > 0;
        this.K = K;
        this.rings = new HashMap<>(K);
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new TreeSet<>(AddressComparator.getComparatorWithSeed(k)));
        }
    }

    /**
     * Used to bootstrap a membership view from the fields of a MembershipView.Configuration object.
     */
    MembershipView(final int K, final Collection<NodeId> nodeIds,
                   final Collection<HostAndPort> hostAndPorts) {
        assert K > 0;
        this.K = K;
        this.rings = new HashMap<>(K);
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new TreeSet<>(AddressComparator.getComparatorWithSeed(k)));
            this.rings.get(k).addAll(hostAndPorts);
        }
        this.identifiersSeen.addAll(nodeIds);
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
    JoinStatusCode isSafeToJoin(final HostAndPort node, final NodeId uuid) {
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
    void ringAdd(final HostAndPort node, final NodeId nodeId) {
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
    void ringDelete(final HostAndPort node) {
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
    List<HostAndPort> getMonitorsOf(final HostAndPort node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            if (!rings.get(0).contains(node)) {
                throw new NodeNotInRingException(node);
            }

            if (rings.get(0).size() <= 1) {
                return Collections.emptyList();
            }

            final List<HostAndPort> monitors = new ArrayList<>();

            for (int k = 0; k < K; k++) {
                final NavigableSet<HostAndPort> list = rings.get(k);
                final HostAndPort successor = list.higher(node);
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
    List<HostAndPort> getMonitoreesOf(final HostAndPort node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            if (!rings.get(0).contains(node)) {
                throw new NodeNotInRingException(node);
            }

            final List<HostAndPort> monitorees = new ArrayList<>();
            if (rings.get(0).size() <= 1) {
                return monitorees;
            }

            for (int k = 0; k < K; k++) {
                final NavigableSet<HostAndPort> list = rings.get(k);
                final HostAndPort predecessor = list.lower(node);
                if (predecessor == null) {
                    monitorees.add(list.last());
                }
                else {
                    monitorees.add(predecessor);
                }
            }
            return monitorees;
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
    List<HostAndPort> getExpectedMonitorsOf(final HostAndPort node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            if (rings.get(0).size() == 0) {
                return Collections.emptyList();
            }

            final List<HostAndPort> monitorees = new ArrayList<>();

            for (int k = 0; k < K; k++) {
                final NavigableSet<HostAndPort> list = rings.get(k);
                final HostAndPort predecessor = list.lower(node);
                if (predecessor == null) {
                    monitorees.add(list.last());
                }
                else {
                    monitorees.add(predecessor);
                }
            }
            return monitorees;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Query if a host is part of the current membership set.
     *
     * @param address the host
     * @return True if the node is present in the membership view and false otherwise.
     */
    boolean isHostPresent(final HostAndPort address) {
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
     * Get the list of hosts in the k'th ring.
     *
     * @param k the index of the ring to query
     * @return the list of hosts in the k'th ring.
     */
    List<HostAndPort> getRing(final int k) {
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
    List<Integer> getRingNumbers(final HostAndPort monitor, final HostAndPort monitoree) {
        rwLock.readLock().lock();
        try {
            // TODO: do this in one scan
            final List<HostAndPort> monitorees = getMonitoreesOf(monitor);
            if (monitorees.size() == 0) {
                return Collections.emptyList();
            }

            final List<Integer> ringIndexes = new ArrayList<>();
            int ringNumber = 0;
            for (final HostAndPort node: monitorees) {
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
        currentConfigurationId = Configuration.getConfigurationId(identifiersSeen, rings.get(0));
    }

    /**
     * Get a Configuration object that contains the list of nodes in the membership view
     * as well as the identifiers seen so far. These two lists suffice to bootstrap an
     * identical copy of the MembershipView object.
     *
     * @return a {@code Configuration} object.
     */
    Configuration getConfiguration() {
        rwLock.readLock().lock();
        try {
            return new Configuration(identifiersSeen, rings.get(0));
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Used to order hosts in the different rings.
     */
    private static final class AddressComparator implements Comparator<HostAndPort>, Serializable {
        private static final long serialVersionUID = -4891729390L;
        private static final Map<Integer, AddressComparator> INSTANCES = new HashMap<>();
        private final LongHashFunction hashFunction;

        AddressComparator(final int seed) {
            this.hashFunction = LongHashFunction.xx(seed);
        }

        public final int compare(final HostAndPort c1, final HostAndPort c2) {
            final long hash1 = hashFunction.hashChars(c1.getHost()) * 31 + hashFunction.hashInt(c1.getPort());
            final long hash2 = hashFunction.hashChars(c2.getHost()) * 31 + hashFunction.hashInt(c2.getPort());
            return Long.compare(hash1, hash2);
        }

        static synchronized AddressComparator getComparatorWithSeed(final int seed) {
            return INSTANCES.computeIfAbsent(seed, AddressComparator::new);
        }
    }

    private static final class NodeIdComparator implements Comparator<NodeId>, Serializable {
        private static final long serialVersionUID = -4891729395L;
        private static final NodeIdComparator INSTANCE = new NodeIdComparator();

        private NodeIdComparator() {
        }

        @Override
        public int compare(final NodeId o1, final NodeId o2) {
            return (o1.getHigh() < o2.getHigh() ? -1 :
                    (o1.getHigh() > o2.getHigh() ? 1 :
                     (o1.getLow() < o2.getLow() ? -1 :
                      (o1.getLow() > o2.getLow() ? 1 :
                        0))));
        }
    }

    static class NodeAlreadyInRingException extends RuntimeException {
        NodeAlreadyInRingException(final HostAndPort node) {
            super(node.toString());
        }
    }

    static class NodeNotInRingException extends RuntimeException {
        NodeNotInRingException(final HostAndPort node) {
            super(node.toString());
        }
    }

    static class UUIDAlreadySeenException extends RuntimeException {
        UUIDAlreadySeenException(final HostAndPort node, final NodeId nodeId) {
            super("Host add attempt with identifier already seen:" +
                    " {host: " + node + ", identifier: " + nodeId + "}:");
        }
    }

    /**
     * The Configuration object contains a list of nodes in the membership view as well as a list of UUIDs.
     * An instance of this object created from one MembershipView object contains the necessary information
     * to bootstrap an identical MembershipView object.
     */
    static class Configuration {
        final List<NodeId> nodeIds;
        final List<HostAndPort> hostAndPorts;

        public Configuration(final Set<NodeId> nodeIds, final Set<HostAndPort> hostAndPorts) {
            this.nodeIds = ImmutableList.copyOf(nodeIds);
            this.hostAndPorts = ImmutableList.copyOf(hostAndPorts);
        }

        /**
         * Gets the configuration ID for the list of hosts and identifiers.
         *
         * @return a configuration identifier.
         */
        public long getConfigurationId() {
            return getConfigurationId(this.nodeIds, this.hostAndPorts);
        }

        static long getConfigurationId(final Collection<NodeId> identifiers,
                                       final Collection<HostAndPort> hostAndPorts) {
            long hash = 1;
            for (final NodeId id: identifiers) {
                hash = hash * 37 + HASH_FUNCTION.hashLong(id.getHigh());
                hash = hash * 37 + HASH_FUNCTION.hashLong(id.getLow());
            }
            for (final HostAndPort hostAndPort: hostAndPorts) {
                hash = hash * 37 + HASH_FUNCTION.hashChars(hostAndPort.getHost());
                hash = hash * 37 + HASH_FUNCTION.hashInt(hostAndPort.getPort());
            }
            return hash;
        }

    }
}