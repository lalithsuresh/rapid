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

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.vrg.rapid.monitoring.ILinkFailureDetectorFactory;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a simple ping-pong failure detector. It is also aware of nodes that are added to the cluster
 * but are still bootstrapping.
 */
@NotThreadSafe
public class PingPongFailureDetector implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PingPongFailureDetector.class);
    private static final int FAILURE_THRESHOLD = 10;

    // Number of BOOTSTRAPPING status responses a node is allowed to return before we begin
    // treating that as a failure condition.
    private static final int BOOTSTRAP_COUNT_THRESHOLD = 30;
    private final HostAndPort address;
    private final HostAndPort monitoree;
    private final AtomicInteger failureCount;
    private final AtomicInteger bootstrapResponseCount;
    private final RpcClient rpcClient;
    private final Runnable notifier;

    // A cache for probe messages. Avoids creating an unnecessary copy of a probe message each time.
    private final ProbeMessage probeMessage;

    private PingPongFailureDetector(final HostAndPort address, final HostAndPort monitoree, final RpcClient rpcClient,
                                    final Runnable notifier) {
        this.address = address;
        this.monitoree = monitoree;
        this.rpcClient = rpcClient;
        this.notifier = notifier;
        this.failureCount = new AtomicInteger(0);
        this.bootstrapResponseCount = new AtomicInteger(0);
        this.probeMessage = ProbeMessage.newBuilder().setSender(address.toString()).build();
    }

    // Executed at monitor
    private boolean hasFailed() {
        return failureCount.get() >= FAILURE_THRESHOLD;
    }

    @Override
    public void run() {
        if (hasFailed()) {
            notifier.run();
        }
        else {
            LOG.trace("{} sending probe to {}", address, monitoree);
            Futures.addCallback(rpcClient.sendProbeMessage(monitoree, probeMessage),
                    new ProbeCallback(monitoree));
        }
    }

    private class ProbeCallback implements FutureCallback<ProbeResponse> {
        final HostAndPort monitoree;

        ProbeCallback(final HostAndPort monitoree) {
            this.monitoree = monitoree;
        }

        @Override
        public void onSuccess(@Nullable final ProbeResponse probeResponse) {
            if (probeResponse == null) {
                handleProbeOnFailure(new RuntimeException("null probe response received"));
                return;
            }
            if (probeResponse.getStatus().equals(NodeStatus.BOOTSTRAPPING)) {
                final int numBootstrapResponses = bootstrapResponseCount.incrementAndGet();
                if (numBootstrapResponses > BOOTSTRAP_COUNT_THRESHOLD) {
                    handleProbeOnFailure(new RuntimeException("BOOTSTRAP_COUNT_THRESHOLD exceeded"));
                    return;
                }
            }
            handleProbeOnSuccess();
        }

        @Override
        public void onFailure(final Throwable throwable) {
            handleProbeOnFailure(throwable);
        }

        // Executed at monitor
        private void handleProbeOnSuccess() {
            LOG.trace("handleProbeOnSuccess at {} from {}", address, monitoree);
        }

        // Executed at monitor
        private void handleProbeOnFailure(final Throwable throwable) {
            failureCount.incrementAndGet();
            LOG.trace("handleProbeOnFailure at {} from {}: {}", address, monitoree, throwable.getLocalizedMessage());
        }
    }

    static class Factory implements ILinkFailureDetectorFactory {
        private final HostAndPort address;
        private final RpcClient rpcClient;

        Factory(final HostAndPort address, final RpcClient rpcClient) {
            this.address = address;
            this.rpcClient = rpcClient;
        }

        @Override
        public Runnable createInstance(final HostAndPort monitoree, final Runnable notifier) {
            return new PingPongFailureDetector(address, monitoree, rpcClient, notifier);
        }
    }
}
