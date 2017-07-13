package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;

import java.util.Set;

/**
 * Used for testing.
 */
class StaticFailureDetector implements Runnable {
    private final Set<HostAndPort> failedNodes;
    private final HostAndPort monitoree;
    private final Runnable notifier;

    StaticFailureDetector(final HostAndPort monitoree, final Runnable notifier,
                          final Set<HostAndPort> blackList) {
        this.monitoree = monitoree;
        this.notifier = notifier;
        this.failedNodes = blackList;
    }

    private boolean hasFailed() {
        return failedNodes.contains(monitoree);
    }

    @Override
    public void run() {
        if (hasFailed()) {
            notifier.run();
        }
    }

    static class Factory implements ILinkFailureDetectorFactory {
        private final Set<HostAndPort> blackList;

        Factory(final Set<HostAndPort> blackList) {
            this.blackList = blackList;
        }

        @Override
        public Runnable createInstance(final HostAndPort monitor, final Runnable notification) {
            return new StaticFailureDetector(monitor, notification, blackList);
        }

        void addFailedNodes(final Set<HostAndPort> nodes) {
            blackList.addAll(nodes);
        }
    }
}