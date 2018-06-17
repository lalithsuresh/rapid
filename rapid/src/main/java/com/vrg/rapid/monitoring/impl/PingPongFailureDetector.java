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

package com.vrg.rapid.monitoring.impl;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.monitoring.IEdgeFailureDetectorFactory;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
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
    private final Endpoint address;
    private final Endpoint subject;
    private final AtomicInteger failureCount;
    private final AtomicInteger bootstrapResponseCount;
    private final IMessagingClient rpcClient;
    private final Runnable notifier;
    private boolean notified = false;

    // A cache for probe messages. Avoids creating an unnecessary copy of a probe message each time.
    private final RapidRequest probeMessage;

    private PingPongFailureDetector(final Endpoint address, final Endpoint subject,
                                    final IMessagingClient rpcClient, final Runnable notifier) {
        this.address = address;
        this.subject = subject;
        this.rpcClient = rpcClient;
        this.notifier = notifier;
        this.failureCount = new AtomicInteger(0);
        this.bootstrapResponseCount = new AtomicInteger(0);
        this.probeMessage = RapidRequest.newBuilder().setProbeMessage(
                ProbeMessage.newBuilder().setSender(address).build()).build();
    }

    // Executed at observer
    private boolean hasFailed() {
        return failureCount.get() >= FAILURE_THRESHOLD;
    }

    @Override
    public void run() {
        if (hasFailed() && !notified) {
            notified = true;
            notifier.run();
        }
        else {
            LOG.trace("{} sending probe to {}", address, subject);
            Futures.addCallback(rpcClient.sendMessageBestEffort(subject, probeMessage),
                    new ProbeCallback(subject));
        }
    }

    private class ProbeCallback implements FutureCallback<RapidResponse> {
        final Endpoint subject;

        ProbeCallback(final Endpoint subject) {
            this.subject = subject;
        }

        @Override
        public void onSuccess(@Nullable final RapidResponse response) {
            if (response == null) {
                handleProbeOnFailure(new RuntimeException("null probe response received"));
                return;
            }
            final ProbeResponse probeResponse = response.getProbeResponse();
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

        // Executed at observer
        private void handleProbeOnSuccess() {
            LOG.trace("handleProbeOnSuccess at {} from {}", address, subject);
        }

        // Executed at observer
        private void handleProbeOnFailure(final Throwable throwable) {
            failureCount.incrementAndGet();
            LOG.trace("handleProbeOnFailure at {} from {}: {}", address, subject, throwable.getLocalizedMessage());
        }
    }

    public static class Factory implements IEdgeFailureDetectorFactory {
        private final Endpoint address;
        private final IMessagingClient messagingClient;

        public Factory(final Endpoint address, final IMessagingClient messagingClient) {
            this.address = address;
            this.messagingClient = messagingClient;
        }

        @Override
        public Runnable createInstance(final Endpoint subject, final Runnable notifier) {
            return new PingPongFailureDetector(address, subject, messagingClient, notifier);
        }
    }
}
