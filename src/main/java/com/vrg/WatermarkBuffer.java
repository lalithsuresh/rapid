package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by lsuresh on 12/15/16.
 */
public class WatermarkBuffer {
    private final int K;
    private final int H = 8;
    private final int L = 3;
    @NonNull public final AtomicInteger flushCounter = new AtomicInteger(0);
    @NonNull private final AtomicInteger safetyValve = new AtomicInteger(0);
    @NonNull private final Map<InetSocketAddress, AtomicInteger> incarnationNumber;
    @NonNull private final Map<InetSocketAddress, AtomicInteger> updateCounter;
    @NonNull private final ArrayList<Node> readyList = new ArrayList<>();
    @NonNull private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public WatermarkBuffer(final int K) {
        this.K = K;
        this.updateCounter = new HashMap<>();
        this.incarnationNumber = new HashMap<>();
    }

    public int ReceiveLinkUpdateMessage(@NonNull final LinkUpdateMessage msg) {
        try {
            rwLock.writeLock().lock();

            AtomicInteger incarnation = incarnationNumber.get(msg.getSrc());
            if (incarnation == null) {
                incarnation = new AtomicInteger(msg.getIncarnation());
                updateCounter.putIfAbsent(msg.getSrc(), new AtomicInteger(0));
                incarnationNumber.putIfAbsent(msg.getSrc(), incarnation);
            }
            else if (incarnation.get() > msg.getIncarnation()) {
                return -1;
            }
            assert (incarnation.get() >= msg.getIncarnation());

            AtomicInteger counter = updateCounter.get(msg.getSrc());
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
                        updateCounter.get(n.address).getAndSet(0);
                        incarnationNumber.get(n.address).incrementAndGet();
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
}