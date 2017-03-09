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

package com.vrg.rapid.monitoring;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a simple ping-pong failure detector.
 */
public class PingPongFailureDetector implements ILinkFailureDetector {
    private static final Logger LOG = LoggerFactory.getLogger(PingPongFailureDetector.class);
    private static final int FAILURE_THRESHOLD = 3;
    private final HostAndPort address;
    private final ConcurrentHashMap<HostAndPort, AtomicInteger> failureCount;

    public PingPongFailureDetector(final HostAndPort address) {
        this.address = address;
        this.failureCount = new ConcurrentHashMap<>();
    }

    // Executed at monitor
    @Override
    public ProbeMessage createProbe(final HostAndPort monitoree) {
        LOG.trace("{} sending probe to {}", address, monitoree);
        return ProbeMessage.newBuilder().setSender(address.toString()).build();
    }

    // Executed at monitor
    @Override
    public void handleProbeOnSuccess(final ProbeResponse probeResponse,
                                     final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", address, monitoree);
        }
        LOG.trace("handleProbeOnSuccess at {} from {}", address, monitoree);
    }

    // Executed at monitor
    @Override
    public void handleProbeOnFailure(final Throwable throwable,
                                     final HostAndPort monitoree) {
        if (!failureCount.containsKey(monitoree)) {
            LOG.trace("handleProbeOnSuccess at {} heard from a node we are not assigned to ({})", address, monitoree);
        }
        failureCount.get(monitoree).incrementAndGet();
        LOG.trace("handleProbeOnFailure at {} from {}", address, monitoree);
    }

    // Executed at monitor
    @Override
    public void onMembershipChange(final List<HostAndPort> monitorees) {
        failureCount.clear();
        // TODO: If a monitoree is part of both the old and new configuration, we shouldn't forget its failure count.
        for (final HostAndPort node: monitorees) {
            failureCount.put(node, new AtomicInteger(0));
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
    @Override
    public void handleProbeMessage(final ProbeMessage probeMessage,
                                   final StreamObserver<ProbeResponse> probeResponseStreamObserver) {
        LOG.trace("handleProbeMessage at {} from {}", address, probeMessage.getSender());
        probeResponseStreamObserver.onNext(ProbeResponse.getDefaultInstance());
        probeResponseStreamObserver.onCompleted();
    }
}
