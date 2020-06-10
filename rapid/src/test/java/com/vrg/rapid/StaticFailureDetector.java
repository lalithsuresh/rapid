/*
 * Copyright © 2016 - 2020 VMware, Inc. All Rights Reserved.
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

import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.monitoring.IEdgeFailureDetectorFactory;

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

    static class Factory implements IEdgeFailureDetectorFactory {
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