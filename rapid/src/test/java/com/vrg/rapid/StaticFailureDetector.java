package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;

import java.util.Set;

/**
 * Used for testing.
 */
class StaticFailureDetector implements Runnable {
    private final Set<Endpoint> failedNodes;
    private final Endpoint subject;
    private final Runnable notifier;

    private StaticFailureDetector(final Endpoint subject, final Runnable notifier,
                          final Set<Endpoint> blackList) {
        this.subject = subject;
        this.notifier = notifier;
        this.failedNodes = blackList;
    }

    private boolean hasFailed() {
        return failedNodes.contains(subject);
    }

    @Override
    public void run() {
        if (hasFailed()) {
            notifier.run();
        }
    }

    static class Factory implements ILinkFailureDetectorFactory {
        private final Set<Endpoint> blackList;

        Factory(final Set<Endpoint> blackList) {
            this.blackList = blackList;
        }

        @Override
        public Runnable createInstance(final Endpoint observer, final Runnable notification) {
            return new StaticFailureDetector(observer, notification, blackList);
        }

        void addFailedNodes(final Set<Endpoint> nodes) {
            blackList.addAll(nodes);
        }
    }
}