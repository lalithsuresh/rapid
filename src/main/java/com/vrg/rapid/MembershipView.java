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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Hosts K permutations of the memberlist that represent the monitoring
 * relationship between nodes; every node monitors its successor on each ring.
 *
 * TODO: too many scans of the k rings during reads. Maintain a cache.
 */
//@DefaultQualifier(value = NonNull.class, locations = TypeUseLocation.ALL)
class MembershipView {
    private final ConcurrentHashMap<Integer, ArrayList<Node>> rings;
    private final int K;
    private final HashComparator[] hashComparators;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final AtomicInteger nodeAlreadyInRingExceptionsThrown = new AtomicInteger(0);
    private final AtomicInteger nodeNotInRingExceptionsThrown = new AtomicInteger(0);

    MembershipView(final int K) {
        assert K > 0;
        this.K = K;
        this.rings = new ConcurrentHashMap<>(K);
        this.hashComparators = new HashComparator[K];
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new ArrayList<>());
            hashComparators[k] = new HashComparator(Integer.toString(k));
        }
    }

    MembershipView(final int K, final Node node) {
        assert K > 0;
        Objects.requireNonNull(node);
        this.K = K;
        this.rings = new ConcurrentHashMap<>(K);
        this.hashComparators = new HashComparator[K];
        for (int k = 0; k < K; k++) {
            final ArrayList<Node> list = new ArrayList<>();
            list.add(node);
            this.rings.put(k, list);
            hashComparators[k] = new HashComparator(Integer.toString(k));
        }
    }

    @VisibleForTesting
    void ringAdd(final Node node) throws NodeAlreadyInRingException {
        Objects.requireNonNull(node);
        try {
            rwLock.writeLock().lock();
            for (int k = 0; k < K; k++) {
                final ArrayList<Node> list = rings.get(k);
                final int index = Collections.binarySearch(list, node, hashComparators[k]);

                if (index >= 0) {
                    throw new NodeAlreadyInRingException(node);
                }

                // Indexes being changed are (-1 * index - 1) - 1, (-1 * index - 1), (-1 * index - 1) + 1
                final int newNodeIndex = (-1 * index - 1);
                list.add(newNodeIndex, node);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    void ringDelete(final Node node) throws NodeNotInRingException {
        Objects.requireNonNull(node);
        try {
            rwLock.writeLock().lock();
            for (int k = 0; k < K; k++) {
                final ArrayList<Node> list = rings.get(k);
                final int index = Collections.binarySearch(list, node, hashComparators[k]);

                if (index < 0) {
                    throw new NodeNotInRingException(node);
                }

                // Indexes being changed are index - 1, index, index + 1
                list.remove(index);
            }
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
    Set<Node> monitorsOf(final Node node) throws NodeNotInRingException {
        Objects.requireNonNull(node);
        try {
            rwLock.readLock().lock();
            final Set<Node> monitors = new HashSet<>();
            for (int k = 0; k < K; k++) {
                final ArrayList<Node> list = rings.get(k);

                if (list.size() <= 1) {
                    return monitors;
                }

                final int index = Collections.binarySearch(list, node, hashComparators[k]);

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
    Set<Node> monitoreesOf(final Node node) throws NodeNotInRingException {
        Objects.requireNonNull(node);
        try {
            rwLock.readLock().lock();
            final Set<Node> monitorees = new HashSet<>();
            for (int k = 0; k < K; k++) {
                final ArrayList<Node> list = rings.get(k);

                if (list.size() <= 1) {
                    return monitorees;
                }

                final int index = Collections.binarySearch(list, node, hashComparators[k]);

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

    /**
     * Deliver a LinkUpdateMessage
     * @param msg message to deliver
     */
    void deliver(final LinkUpdateMessage msg) {
        Objects.requireNonNull(msg);
        try {
            switch (msg.getStatus()) {
                case UP:
                    ringAdd(new Node(msg.getSrc()));
                    break;
                case DOWN:
                    ringDelete(new Node(msg.getSrc()));
                    break;
                default:
                    // Invalid message
                    assert false;
            }
        } catch (final NodeAlreadyInRingException e) {
            nodeAlreadyInRingExceptionsThrown.incrementAndGet();
        } catch (final NodeNotInRingException e) {
            nodeNotInRingExceptionsThrown.incrementAndGet();
        }
    }

    boolean isPresent(final Node node) {
        final int index = Collections.binarySearch(rings.get(0), node, hashComparators[0]);
        return index >= 0;
    }

    @VisibleForTesting
    List<Node> viewRing(final int k) {
        try {
            rwLock.readLock().lock();
            assert k >= 0;
            return Collections.unmodifiableList(rings.get(k));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private static final class HashComparator implements Comparator<Node>, Serializable {
        private static final long serialVersionUID = -4891729390L;
        private final String seed;

        HashComparator(final String seed) {
            this.seed = seed;
        }

        public final int compare(final Node c1, final Node c2) {
            return Utils.sha1HexStringToString(c1.address.toString() + seed)
                    .compareTo(Utils.sha1HexStringToString(c2.address.toString() + seed));
        }
    }

    static class NodeAlreadyInRingException extends Exception {
        NodeAlreadyInRingException(final Node node) {
            super(node.address.toString());
        }
    }

    static class NodeNotInRingException extends Exception {
        NodeNotInRingException(final Node node) {
            super(node.address.toString());
        }
    }
}