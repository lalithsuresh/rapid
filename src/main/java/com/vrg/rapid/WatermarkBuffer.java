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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.LinkUpdateMessage;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A filter that outputs a view change proposal about a node only if:
 * - there are H reports about a node.
 * - there is no other node about which there are more than L but less than H reports.
 */
final class WatermarkBuffer {
    private static final int K_MIN = 3;
    private final int K;
    private final int H;
    private final int L;
    @GuardedBy("lock") private final AtomicInteger proposalCount = new AtomicInteger(0);
    @GuardedBy("lock") private final AtomicInteger updatesInProgress = new AtomicInteger(0);
    @GuardedBy("lock") private final Map<HostAndPort, Map<Integer, HostAndPort>> reportsPerHost;
    @GuardedBy("lock") private final ArrayList<HostAndPort> proposal = new ArrayList<>();
    private final Object lock = new Object();

    WatermarkBuffer(final int K, final int H, final int L) {
        if (H > K || L > H || K < K_MIN || L <= 0 || H <= 0 || K <= 0) {
            throw new IllegalArgumentException("Arguments do not satisfy K > H >= L >= 0:" +
                                               " (K: " + K + ", H: " + H + ", L: " + L);
        }
        this.K = K;
        this.H = H;
        this.L = L;
        this.reportsPerHost = new HashMap<>();
    }

    int getNumProposals() {
        synchronized (lock) {
            return proposalCount.get();
        }
    }

    /**
     * Apply a LinkUpdateMessage against the Watermark filter. When an update moves a host
     * past the H threshold of reports, and no other host has between H and L reports, the
     * method returns a view change proposal.
     *
     * @param msg A LinkUpdateMessage to apply against the filter
     * @return a list of hosts about which a view change has been recorded. Empty list if there is no proposal.
     */
    List<HostAndPort> aggregateForProposal(final LinkUpdateMessage msg) {
        Objects.requireNonNull(msg);
        assert msg.getRingNumber() <= K;

        synchronized (lock) {
            final HostAndPort linkDst = HostAndPort.fromString(msg.getLinkDst());
            final Map<Integer, HostAndPort> reportsForHost = reportsPerHost.computeIfAbsent(
                                                                 linkDst,
                                                                 (k) -> new HashMap<>(K));

            if (reportsForHost.containsKey(msg.getRingNumber())) {
                return Collections.emptyList();  // duplicate announcement, ignore.
            }

            reportsForHost.put(msg.getRingNumber(), HostAndPort.fromString(msg.getLinkSrc()));
            final int numReportsForHost = reportsForHost.size();

            if (numReportsForHost == L) {
                updatesInProgress.incrementAndGet();
            }

            if (numReportsForHost == H) {
                // Enough reports about "msg.getDst()" have been received that it is safe to act upon,
                // provided there are no other nodes with L < #reports < H.
                proposal.add(linkDst);
                final int updatesInProgressVal = updatesInProgress.decrementAndGet();

                if (updatesInProgressVal == 0) {
                    // No outstanding updates, so all nodes that have crossed the H threshold of reports are
                    // now part of a single proposal.
                    this.proposalCount.incrementAndGet();
                    for (final HostAndPort n : proposal) {
                        // The counter below should never be null.
                        final Map<Integer, HostAndPort> reportsSet = reportsPerHost.get(n);
                        if (reportsSet == null) {
                            throw new RuntimeException("Host for proposal not in UpdateCounters map: " + n);
                        }
                    }
                    final List<HostAndPort> ret = ImmutableList.copyOf(proposal);
                    proposal.clear();
                    return ret;
                }
            }

            return Collections.emptyList();
        }
    }

    /**
     * Clears all view change reports being tracked. To be used right after a view change.
     */
    void clear() {
        synchronized (lock) {
            reportsPerHost.clear();
            proposal.clear();
            updatesInProgress.set(0);
            proposalCount.set(0);
        }
    }
}