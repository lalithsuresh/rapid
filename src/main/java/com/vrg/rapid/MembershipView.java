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
import com.google.common.net.HostAndPort;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
class MembershipView {
    private static final int INITIAL_CAPACITY = 10000;

    private final ConcurrentHashMap<Integer, ArrayList<HostAndPort>> rings;
    private final int K;
    private final AddressComparator[] addressComparators;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Set<UUID> identifiersSeen = new HashSet<>();
    private long currentConfigurationId;
    private boolean shouldUpdateConfigurationId = true;

    MembershipView(final int K) {
        assert K > 0;
        this.K = K;
        this.rings = new ConcurrentHashMap<>(K);
        this.addressComparators = new AddressComparator[K];
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new ArrayList<>());
            addressComparators[k] = new AddressComparator(Integer.toString(k));
        }
    }

    MembershipView(final int K, final HostAndPort node) {
        assert K > 0;
        Objects.requireNonNull(node);
        this.K = K;
        this.rings = new ConcurrentHashMap<>(K);
        this.addressComparators = new AddressComparator[K];
        for (int k = 0; k < K; k++) {
            final ArrayList<HostAndPort> list = new ArrayList<>();
            list.add(node);
            this.rings.put(k, list);
            addressComparators[k] = new AddressComparator(Integer.toString(k));
        }
    }

    @VisibleForTesting
    void ringAdd(final HostAndPort node, final UUID uuid) throws NodeAlreadyInRingException {
        Objects.requireNonNull(node);
        Objects.requireNonNull(uuid);

        if (identifiersSeen.contains(uuid)) {
            throw new RuntimeException("Host add attempt with identifier already seen:" +
                    " {host: " + node + ", identifier: " + uuid + "}:");
        }

        try {
            rwLock.writeLock().lock();
            for (int k = 0; k < K; k++) {
                final ArrayList<HostAndPort> list = rings.get(k);
                final int index = Collections.binarySearch(list, node, addressComparators[k]);

                if (index >= 0) {
                    throw new NodeAlreadyInRingException(node);
                }

                // Indexes being changed are (-1 * index - 1) - 1, (-1 * index - 1), (-1 * index - 1) + 1
                final int newNodeIndex = (-1 * index - 1);
                list.add(newNodeIndex, node);
            }
            identifiersSeen.add(uuid);
            shouldUpdateConfigurationId = true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    void ringDelete(final HostAndPort node) throws NodeNotInRingException {
        Objects.requireNonNull(node);
        try {
            rwLock.writeLock().lock();
            for (int k = 0; k < K; k++) {
                final ArrayList<HostAndPort> list = rings.get(k);
                final int index = Collections.binarySearch(list, node, addressComparators[k]);

                if (index < 0) {
                    throw new NodeNotInRingException(node);
                }

                // Indexes being changed are index - 1, index, index + 1
                list.remove(index);
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
    Set<HostAndPort> monitorsOf(final HostAndPort node) throws NodeNotInRingException {
        Objects.requireNonNull(node);
        try {
            rwLock.readLock().lock();
            final Set<HostAndPort> monitors = new HashSet<>();
            for (int k = 0; k < K; k++) {
                final ArrayList<HostAndPort> list = rings.get(k);

                if (list.size() <= 1) {
                    return monitors;
                }

                final int index = Collections.binarySearch(list, node, addressComparators[k]);

                if (index < 0) {
                    throw new NodeNotInRingException(node);
                }

                monitors.add(list.get(Math.floorMod(index - 1, list.size()))); // Handles wrap around
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
    Set<HostAndPort> monitoreesOf(final HostAndPort node) throws NodeNotInRingException {
        Objects.requireNonNull(node);
        try {
            rwLock.readLock().lock();
            final Set<HostAndPort> monitorees = new HashSet<>();
            for (int k = 0; k < K; k++) {
                final ArrayList<HostAndPort> list = rings.get(k);

                if (list.size() <= 1) {
                    return monitorees;
                }

                final int index = Collections.binarySearch(list, node, addressComparators[k]);

                if (index < 0) {
                    throw new NodeNotInRingException(node);
                }

                monitorees.add(list.get((index + 1) % list.size())); // Handles wrap around
            }
            return monitorees;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    boolean isPresent(final HostAndPort address) {
        final int index = Collections.binarySearch(rings.get(0), address, addressComparators[0]);
        return index >= 0;
    }

    long getCurrentConfigurationId() {
        try {
            rwLock.readLock().lock();
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
    List<HostAndPort> viewRing(final int k) {
        try {
            rwLock.readLock().lock();
            assert k >= 0;
            return Collections.unmodifiableList(rings.get(k));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private void updateCurrentConfigurationId() {
        long result = Utils.murmurHex(identifiersSeen);
        for (final HostAndPort address: rings.get(0)) {
            result = result * 31 + Utils.murmurHex(address.toString());
        }
        currentConfigurationId = result;
    }

    private static final class AddressComparator implements Comparator<HostAndPort>, Serializable {
        private static final long serialVersionUID = -4891729390L;
        private final String seed;

        AddressComparator(final String seed) {
            this.seed = seed;
        }

        public final int compare(final HostAndPort c1, final HostAndPort c2) {
            return Utils.sha1Hex(c1.toString() + seed)
                    .compareTo(Utils.sha1Hex(c2.toString() + seed));
        }
    }

    static class NodeAlreadyInRingException extends Exception {
        NodeAlreadyInRingException(final HostAndPort node) {
            super(node.toString());
        }
    }

    static class NodeNotInRingException extends Exception {
        NodeNotInRingException(final HostAndPort node) {
            super(node.toString());
        }
    }
}