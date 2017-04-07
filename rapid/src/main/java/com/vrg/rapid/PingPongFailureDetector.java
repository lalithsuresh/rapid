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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.vrg.rapid.monitoring.ILinkFailureDetector;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a simple ping-pong failure detector. It is also aware of nodes that are added to the cluster
 * but are still bootstrapping.
 */
@NotThreadSafe
public class PingPongFailureDetector implements ILinkFailureDetector {
    private static final Logger LOG = LoggerFactory.getLogger(PingPongFailureDetector.class);
    private static final Executor BACKGROUND_EXECUTOR = Executors.newSingleThreadScheduledExecutor();
    private static final int FAILURE_THRESHOLD = 10;

    // Number of BOOTSTRAPPING status responses a node is allowed to return before we begin
    // treating that as a failure condition.
    private static final int BOOTSTRAP_COUNT_THRESHOLD = 30;
    private final HostAndPort address;
    private final ConcurrentHashMap<HostAndPort, AtomicInteger> failureCount;
    private final ConcurrentHashMap<HostAndPort, AtomicInteger> bootstrapResponseCount;
    private final RpcClient rpcClient;

    // A cache for probe messages. Avoids creating an unnecessary copy of a probe message each time.
    private final HashMap<HostAndPort, ProbeMessage> messageHashMap;

    public PingPongFailureDetector(final HostAndPort address,
                                   final RpcClient rpcClient) {
        this.address = address;
        this.failureCount = new ConcurrentHashMap<>();
        this.bootstrapResponseCount = new ConcurrentHashMap<>();
        this.messageHashMap = new HashMap<>();
        this.rpcClient = rpcClient;
    }

    // Executed at monitor
    @Override
    public ListenableFuture<Void> checkMonitoree(final HostAndPort monitoree) {
        LOG.trace("{} sending probe to {}", address, monitoree);
        final ProbeMessage probeMessage = messageHashMap.get(monitoree);
        final SettableFuture<Void> completionEvent = SettableFuture.create();
        Futures.addCallback(rpcClient.sendProbeMessage(monitoree, probeMessage),
                            new ProbeCallback(monitoree, completionEvent), BACKGROUND_EXECUTOR);
        return completionEvent;
    }

    // Executed at monitor
    private void handleProbeOnSuccess(final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", address, monitoree);
        }
        LOG.trace("handleProbeOnSuccess at {} from {}", address, monitoree);
    }

    // Executed at monitor
    private void handleProbeOnFailure(final Throwable throwable,
                                      final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", address, monitoree);
        }
        failureCount.get(monitoree).incrementAndGet();
        LOG.trace("handleProbeOnFailure at {} from {}: {}", address, monitoree, throwable.getLocalizedMessage());
    }

    // Executed at monitor
    @Override
    public void onMembershipChange(final List<HostAndPort> monitorees) {
        failureCount.clear();
        messageHashMap.clear();
        // TODO: If a monitoree is part of both the old and new configuration, we shouldn't forget its failure count.
        final ProbeMessage.Builder builder = ProbeMessage.newBuilder();
        for (final HostAndPort node: monitorees) {
            failureCount.put(node, new AtomicInteger(0));
            messageHashMap.putIfAbsent(node, builder.setSender(address.toString()).build());
        }
    }

    // Executed at monitor
    @Override
    public boolean hasFailed(final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})",
                       address, monitoree);
        }
        return failureCount.get(monitoree).get() >= FAILURE_THRESHOLD;
    }

    // Executed at monitoree
    public void handleProbeMessage(final ProbeMessage probeMessage,
                                   final StreamObserver<ProbeResponse> probeResponseStreamObserver) {
        LOG.trace("handleProbeMessage at {} from {}", address, probeMessage.getSender());
        probeResponseStreamObserver.onNext(ProbeResponse.getDefaultInstance());
        probeResponseStreamObserver.onCompleted();
    }

    private class ProbeCallback implements FutureCallback<ProbeResponse> {
        final HostAndPort monitoree;
        final SettableFuture<Void> completionEvent;

        ProbeCallback(final HostAndPort monitoree, final SettableFuture<Void> completionEvent) {
            this.monitoree = monitoree;
            this.completionEvent = completionEvent;
        }

        @Override
        public void onSuccess(@Nullable final ProbeResponse probeResponse) {
            try {
                if (probeResponse == null) {
                    handleProbeOnFailure(new RuntimeException("null probe response received"), monitoree);
                    return;
                }
                if (probeResponse.getStatus().equals(NodeStatus.BOOTSTRAPPING)) {
                    final int numBootstrapResponses = bootstrapResponseCount.computeIfAbsent(monitoree,
                            (k) -> new AtomicInteger(0)).incrementAndGet();
                    if (numBootstrapResponses > BOOTSTRAP_COUNT_THRESHOLD) {
                        handleProbeOnFailure(new RuntimeException("BOOTSTRAP_COUNT_THRESHOLD exceeded"), monitoree);
                        return;
                    }
                }
                handleProbeOnSuccess(monitoree);
            } finally {
                completionEvent.set(null);
            }
        }

        @Override
        public void onFailure(final Throwable throwable) {
            try {
                handleProbeOnFailure(throwable, monitoree);
            } finally {
                completionEvent.set(null);
            }
        }
    }
}
