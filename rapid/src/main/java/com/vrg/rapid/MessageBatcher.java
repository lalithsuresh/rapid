package com.vrg.rapid;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Collects messages to be sent in batches at a regular interval
 *
 * @param <T> the type of messages
 */
public abstract class MessageBatcher<T> {

    private long lastEnqueueTimestamp = -1;
    @GuardedBy("batchQueueLock")
    private final LinkedBlockingQueue<T> sendQueue = new LinkedBlockingQueue<>();
    private final Lock batchQueueLock = new ReentrantLock();
    private final ScheduledExecutorService backgroundTasksExecutor;
    @Nullable private ScheduledFuture<?> batchingJob;
    private final int batchingWindowInMs;

    public MessageBatcher(final SharedResources sharedResources, final int batchingWindowInMs) {
        this.batchingWindowInMs = batchingWindowInMs;
        this.backgroundTasksExecutor = sharedResources.getScheduledTasksExecutor();
    }

    /**
     * Sends the batch of messages
     */
    public abstract void sendMessages(final List<T> messages);

    /**
     * Enqueue a messages to be sent in batch
     */
    public void enqueueMessage(final T msg) {
        batchQueueLock.lock();
        try {
            lastEnqueueTimestamp = System.currentTimeMillis();
            sendQueue.add(msg);
        }
        finally {
            batchQueueLock.unlock();
        }
    }

    public void start() {
        batchingJob = this.backgroundTasksExecutor.scheduleAtFixedRate(
                new BatchingJob(),0, batchingWindowInMs, TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        if (batchingJob != null) {
            batchingJob.cancel(false);

        }
    }

    class BatchingJob implements Runnable {
        @Override
        public void run() {
            final ArrayList<T> messages = new ArrayList<>(sendQueue.size());
            batchQueueLock.lock();
            try {
                // Wait one BATCHING_WINDOW_IN_MS since last add before sending out
                if (!sendQueue.isEmpty() && lastEnqueueTimestamp > 0
                        && (System.currentTimeMillis() - lastEnqueueTimestamp) >
                        batchingWindowInMs) {
                    final int numDrained = sendQueue.drainTo(messages);
                    assert numDrained > 0;
                }
            } finally {
                batchQueueLock.unlock();
            }
            if (!messages.isEmpty()) {
                sendMessages(messages);
            }

        }
    }

}