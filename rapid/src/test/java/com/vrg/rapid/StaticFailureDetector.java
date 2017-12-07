package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;

import java.util.Set;

/**
 * Used for testing.
 */
class StaticFailureDetector implements Runnable {
    private final Set<Endpoint> failedNodes;
    private final Endpoint monitoree;
    private final Runnable notifier;

    private StaticFailureDetector(final Endpoint monitoree, final Runnable notifier,
                          final Set<Endpoint> blackList) {
        this.monitoree = monitoree.newBuilderForType().setHostname(monitoree.getHostname())
                                                      .setPort(monitoree.getPort()).build();
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
        private final Set<Endpoint> blackList;

        Factory(final Set<Endpoint> blackList) {
            this.blackList = blackList;
        }

        @Override
        public Runnable createInstance(final Endpoint monitor, final Runnable notification) {
            return new StaticFailureDetector(monitor, notification, blackList);
        }

        void addFailedNodes(final Set<Endpoint> nodes) {
            blackList.addAll(nodes);
        }
    }
}