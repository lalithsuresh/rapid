package com.vrg;

import com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;
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
public class MembershipView {
    @NonNull private final ConcurrentHashMap<Integer, ArrayList<Node>> rings;
    @NonNull private final int K;
    @NonNull private final HashComparator[] hashComparators;
    @NonNull private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    @NonNull private boolean initializedWithSelf = false;
    @NonNull private final AtomicInteger nodeAlreadyInRingExceptionsThrown = new AtomicInteger(0);
    @NonNull private final AtomicInteger nodeNotInRingExceptionsThrown = new AtomicInteger(0);

    public MembershipView(final int K) {
        assert K > 0;
        this.K = K;
        this.rings = new ConcurrentHashMap<>(K);
        this.hashComparators = new HashComparator[K];
        for (int k = 0; k < K; k++) {
            this.rings.put(k, new ArrayList<>());
            hashComparators[k] = new HashComparator(Integer.toString(k));
        }
    }

    @VisibleForTesting
    void ringAdd(@NonNull final Node node) throws NodeAlreadyInRingException {
        try {
            rwLock.writeLock().lock();
            for (int k = 0; k < K; k++) {
                ArrayList<Node> list = rings.get(k);
                final int index = Collections.binarySearch(list, node, hashComparators[k]);

                if (index >= 0) {
                    throw new NodeAlreadyInRingException(node);
                }

                final int newNodeIndex = (-1 * index - 1);
                // Indexes being changed are (-1 * index - 1) - 1, (-1 * index - 1), (-1 * index - 1) + 1
                list.add(newNodeIndex, node);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @VisibleForTesting
    void ringDelete(@NonNull final Node node) throws NodeNotInRingException {
        try {
            rwLock.writeLock().lock();
            for (int k = 0; k < K; k++) {
                ArrayList<Node> list = rings.get(k);
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

    @NonNull public Set<Node> monitorsOf(@NonNull final Node node) throws NodeNotInRingException {
        try {
            rwLock.readLock().lock();
            Set<Node> monitors = new HashSet<>();
            for (int k = 0; k < K; k++) {
                ArrayList<Node> list = rings.get(k);

                if (list.size() <= 1) {
                    return monitors;
                }

                final int index = Collections.binarySearch(list, node, hashComparators[k]);

                if (index < 0) {
                    throw new NodeNotInRingException(node);
                }

                monitors.add(list.get(Math.floorMod(index - 1, list.size())));
            }
            return monitors;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @NonNull public Set<Node> monitoreesOf(@NonNull final Node node) throws NodeNotInRingException {
        try {
            rwLock.readLock().lock();
            Set<Node> monitorees = new HashSet<>();
            for (int k = 0; k < K; k++) {
                ArrayList<Node> list = rings.get(k);

                if (list.size() <= 1) {
                    return monitorees;
                }

                final int index = Collections.binarySearch(list, node, hashComparators[k]);

                if (index < 0) {
                    throw new NodeNotInRingException(node);
                }

                monitorees.add(list.get((index + 1) % list.size()));
            }
            return monitorees;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void deliver(LinkUpdateMessage msg) {
        try {
            switch (msg.getStatus()) {
                case UP:
                    ringAdd(new Node(msg.getSrc()));
                    break;
                case DOWN:
                    ringDelete(new Node(msg.getSrc()));
                    break;
            }
        } catch (NodeAlreadyInRingException e) {
            nodeAlreadyInRingExceptionsThrown.incrementAndGet();
        } catch (NodeNotInRingException e) {
            nodeNotInRingExceptionsThrown.incrementAndGet();
        }

    }

    public void initializeWithSelf(@NonNull final Node node) {
        if (!initializedWithSelf) {
            // The only case a ringAdd is allowed to be called without going
            // through the watermark.
            try {
                ringAdd(node);
            } catch (NodeAlreadyInRingException e) {
                // Should never happen
                assert false;
            }
            initializedWithSelf = true;
        }
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

    private static class HashComparator implements Comparator<Node>
    {
        private final String seed;
        public HashComparator(@NonNull final String seed) {
            this.seed = seed;
        }

        public int compare(@NonNull final Node c1, @NonNull final Node c2) {
            return Utils.sha1Hex(c1.address.toString() + seed)
                    .compareTo(Utils.sha1Hex(c2.address.toString() + seed));
        }
    }

    class NodeAlreadyInRingException extends Exception
    {
        public NodeAlreadyInRingException(Node node)
        {
            super(node.address.toString());
        }
    }

    class NodeNotInRingException extends Exception
    {
        public NodeNotInRingException(Node node)
        {
            super(node.address.toString());
        }
    }
}