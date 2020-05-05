package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;
import net.openhft.hashing.LongHashFunction;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Simple consistent Hash implementation, adapted for Endpoints
 *
 * https://tom-e-white.com/2007/11/consistent-hashing.html
 */
public class ConsistentHash<T> {

    private final LongHashFunction hashFunction = LongHashFunction.xx();
    private final int numberOfReplicas;
    private final SortedMap<Long, Endpoint> circle = new TreeMap<>();

    public ConsistentHash(final int numberOfReplicas, final Collection<Endpoint> nodes) {
        this.numberOfReplicas = numberOfReplicas;

        for (final Endpoint node : nodes) {
            add(node);
        }
    }

    public void add(final Endpoint node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.put(hash(node, i), node);
        }
    }

    public void remove(final Endpoint node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hash(node, i));
        }
    }

    public boolean isEmpty() {
        return circle.isEmpty();
    }

    @Nullable
    public Endpoint get(final Endpoint key) {
        if (circle.isEmpty()) {
            return null;
        }
        long hash = hash(key, 0);
        if (!circle.containsKey(hash)) {
            final SortedMap<Long, Endpoint> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private long hash(final Endpoint node, final int index) {
        return hashFunction.hashBytes(node.getHostname().asReadOnlyByteBuffer()) * 31
                + hashFunction.hashInt(node.getPort()) + hashFunction.hashInt(index);
    }

}