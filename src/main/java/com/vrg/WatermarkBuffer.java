package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.framework.qual.DefaultQualifier;
import org.checkerframework.framework.qual.TypeUseLocation;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Created by lsuresh on 12/15/16.
 */
@DefaultQualifier(value = NonNull.class, locations = TypeUseLocation.ALL)
public class WatermarkBuffer {
    private static final int K_MIN = 3;
    private final int K;
    private final int H;
    private final int L;
    private final AtomicInteger flushCounter = new AtomicInteger(0);
    private final AtomicInteger safetyValve = new AtomicInteger(0);
    private final Map<InetSocketAddress, AtomicInteger> incarnationNumbers;
    private final Map<InetSocketAddress, AtomicInteger> updateCounters;
    private final ArrayList<Node> readyList = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Consumer<LinkUpdateMessage> deliverCallback;

    public WatermarkBuffer(final int K, final int H, final int L,
                           Consumer<LinkUpdateMessage> deliverCallback) {
        if (H > K || L > H || K < K_MIN) {
            throw new IllegalArgumentException("Arguments do not satisfy K > H >= L >= 0:" +
                                               " (K: " + K + ", H: " + H + ", L: " + L);
        }
        this.K = K;
        this.H = H;
        this.L = L;
        this.updateCounters = new HashMap<>();
        this.incarnationNumbers = new HashMap<>();
        this.deliverCallback = deliverCallback;
    }

    public final int ReceiveLinkUpdateMessage(final LinkUpdateMessage msg) {
        try {
            rwLock.writeLock().lock();

            AtomicInteger incarnation = incarnationNumbers.get(msg.getSrc());
            if (incarnation == null) {
                incarnation = new AtomicInteger(msg.getIncarnation());
                updateCounters.putIfAbsent(msg.getSrc(), new AtomicInteger(0));
                incarnationNumbers.putIfAbsent(msg.getSrc(), incarnation);
            }
            else if (incarnation.get() > msg.getIncarnation()) {
                return -1;
            }
            // this calls for an invalidation? I don't think so
            assert (incarnation.get() >= msg.getIncarnation());

            AtomicInteger counter = updateCounters.get(msg.getSrc());
            final int value = counter.incrementAndGet();

            if (value == L) {
                safetyValve.incrementAndGet();
            }

            if (value == H) {
                readyList.add(new Node(msg.getSrc()));
                final int safetyValveValue = safetyValve.decrementAndGet();

                if (safetyValveValue == 0) {
                    this.flushCounter.incrementAndGet();
                    final int flushCount = readyList.size();
                    for (Node n: readyList) {
                        updateCounters.get(n.address).getAndSet(0);
                        incarnationNumbers.get(n.address).incrementAndGet();
                        deliverCallback.accept(msg);
                    }
                    readyList.clear();
                    return flushCount;
                }
            }

            return 0;
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

   public int getNumDelivers() {
        return flushCounter.get();
    }
}