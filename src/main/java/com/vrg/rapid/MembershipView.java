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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.JoinStatusCode;

import javax.annotation.concurrent.GuardedBy;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Hosts K permutations of the memberlist that represent the monitoring
 * relationship between nodes; every node monitors its successor on each ring.
 *
 * TODO: too many scans of the k rings during reads. Maintain a cache.
 */
final class MembershipView {
    private final int K;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    @GuardedBy("rwLock") private final ConcurrentHashMap<Integer, NavigableSet<HostAndPort>> rings;
    @GuardedBy("rwLock") private final Set<UUID> identifiersSeen = new TreeSet<>();
    @GuardedBy("rwLock") private long currentConfigurationId = -1;
    @GuardedBy("rwLock") private boolean shouldUpdateConfigurationId = true;

    MembershipView(final int K) {
        assert K > 0;
        this.K = K;
        this.rings = new ConcurrentHashMap<>(K);
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new TreeSet<>(new AddressComparator(k)));
        }
    }

    /**
     * Used during bootstrapping process.
     */
    MembershipView(final int K, final Collection<UUID> uuids,
                   final Collection<HostAndPort> hostAndPorts) {
        assert K > 0;
        this.K = K;
        this.rings = new ConcurrentHashMap<>(K);
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new TreeSet<>(new AddressComparator(k)));
            this.rings.get(k).addAll(hostAndPorts);
        }
        this.identifiersSeen.addAll(uuids);
    }

    JoinStatusCode isSafeToJoin(final HostAndPort node, final UUID uuid) {
        if (rings.get(0).contains(node)) {
            return JoinStatusCode.HOSTNAME_ALREADY_IN_RING;
        }

        if (identifiersSeen.contains(uuid)) {
            return JoinStatusCode.UUID_ALREADY_IN_RING;
        }

        return JoinStatusCode.SAFE_TO_JOIN;
    }

    @VisibleForTesting
    void ringAdd(final HostAndPort node, final UUID uuid) {
        Objects.requireNonNull(node);
        Objects.requireNonNull(uuid);

        if (identifiersSeen.contains(uuid)) {
            throw new RuntimeException("Host add attempt with identifier already seen:" +
                    " {host: " + node + ", identifier: " + uuid + "}:");
        }

        rwLock.writeLock().lock();
        try {
            if (rings.get(0).contains(node)) {
                throw new NodeAlreadyInRingException(node);
            }

            for (int k = 0; k < K; k++) {
                rings.get(k).add(node);
            }

            identifiersSeen.add(uuid);
            shouldUpdateConfigurationId = true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
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
    List<HostAndPort> monitorsOf(final HostAndPort node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            if (!rings.get(0).contains(node)) {
                throw new NodeNotInRingException(node);
            }

            final List<HostAndPort> monitors = new ArrayList<>();
            if (rings.get(0).size() <= 1) {
                return monitors;
            }

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
    List<HostAndPort> monitoreesOf(final HostAndPort node) {
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
     * @return the set of nodes monitored by {@code node}
     */
    List<HostAndPort> expectedMonitorsOf(final HostAndPort node) {
        Objects.requireNonNull(node);
        rwLock.readLock().lock();
        try {
            final List<HostAndPort> monitorees = new ArrayList<>();
            if (rings.get(0).size() == 0) {
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

    boolean isHostPresent(final HostAndPort address) {
        return rings.get(0).contains(address);
    }

    boolean isIdentifierPresent(final UUID identifier) {
        return identifiersSeen.contains(identifier);
    }

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

    @VisibleForTesting
    List<HostAndPort> getRing(final int k) {
        rwLock.readLock().lock();
        try {
            assert k >= 0;
            return ImmutableList.copyOf(rings.get(k));
        } finally {
            rwLock.readLock().unlock();
        }
    }

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
    private void updateCurrentConfigurationId() {
        currentConfigurationId = Configuration.getConfigurationId(identifiersSeen, rings.get(0));
    }

    Configuration getConfiguration() {
        rwLock.readLock().lock();
        try {
            return new Configuration(identifiersSeen, rings.get(0));
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    private static final class AddressComparator implements Comparator<HostAndPort>, Serializable {
        private static final long serialVersionUID = -4891729390L;
        private final long seed;

        AddressComparator(final long seed) {
            this.seed = seed;
        }

        public final int compare(final HostAndPort c1, final HostAndPort c2) {
            return Long.compare(Utils.murmurHex(c1, seed), Utils.murmurHex(c2, seed));
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

    static class Configuration {
        final List<UUID> uuids;
        final List<HostAndPort> hostAndPorts;

        public Configuration(final Set<UUID> uuids, final Set<HostAndPort> hostAndPorts) {
            this.uuids = ImmutableList.copyOf(uuids);
            this.hostAndPorts = ImmutableList.copyOf(hostAndPorts);
        }

        public long getConfigurationId() {
            return getConfigurationId(this.uuids, this.hostAndPorts);
        }

        static long getConfigurationId(final Collection<UUID> identifiers,
                                       final Collection<HostAndPort> hostAndPorts) {
            int hash = 1;
            for (final UUID id: identifiers) {
                hash = hash * 31 + id.hashCode();
            }
            for (final HostAndPort hostAndPort: hostAndPorts) {
                hash = hash * 31 + hostAndPort.hashCode();
            }
            return hash;
        }

    }
}