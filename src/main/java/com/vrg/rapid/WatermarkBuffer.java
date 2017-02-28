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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A filter that outputs a view change proposal about a node only if:
 * - there are H reports about a node.
 * - there is no other node about which there are more than L but less than H reports.
 */
class WatermarkBuffer {
    private static final int K_MIN = 3;
    private final int K;
    private final int H;
    private final int L;
    private final AtomicInteger proposalCount = new AtomicInteger(0);
    private final AtomicInteger updatesInProgress = new AtomicInteger(0);
    private final Map<Node, Map<Integer, HostAndPort>> reportsPerHost;
    private final ArrayList<Node> proposal = new ArrayList<>();
    private final Object lock = new Object();
    private static final List<Node> EMPTY_LIST =
            Collections.unmodifiableList(new ArrayList<Node>());

    WatermarkBuffer(final int K, final int H, final int L) {
        if (H > K || L > H || K < K_MIN) {
            throw new IllegalArgumentException("Arguments do not satisfy K > H >= L >= 0:" +
                                               " (K: " + K + ", H: " + H + ", L: " + L);
        }
        this.K = K;
        this.H = H;
        this.L = L;
        this.reportsPerHost = new HashMap<>();
    }

    int getNumProposals() {
        return proposalCount.get();
    }

    List<Node> aggregateForProposal(final LinkUpdateMessage msg) {
        Objects.requireNonNull(msg);
        assert msg.getRingNumber() <= K;

        synchronized (lock) {
            final Node node = new Node(msg.getDst(), msg.getUuid());
            final Map<Integer, HostAndPort> reportsForHost = reportsPerHost.computeIfAbsent(
                                             node,
                                             (k) -> new HashMap<>(K));

            if (reportsForHost.containsKey(msg.getRingNumber())) {
                return EMPTY_LIST;  // duplicate announcement, ignore.
            }

            reportsForHost.put(msg.getRingNumber(), msg.getSrc());
            final int numReportsForHost = reportsForHost.size();

            if (numReportsForHost == L) {
                updatesInProgress.incrementAndGet();
            }

            if (numReportsForHost == H) {
                 // Enough reports about "msg.getDst()" have been received that it is safe to act upon,
                 // provided there are no other nodes with L < #reports < H.
                proposal.add(node);
                final int updatesInProgressVal = updatesInProgress.decrementAndGet();

                if (updatesInProgressVal == 0) {
                    // No outstanding updates, so all nodes that have crossed the H threshold of reports are
                    // now part of a single proposal.
                    this.proposalCount.incrementAndGet();
                    for (final Node n: proposal) {
                        // The counter below should never be null.
                        final Map<Integer, HostAndPort> reportsSet = reportsPerHost.get(n);
                        if (reportsSet == null) {
                            throw new RuntimeException("Host for proposal not in UpdateCounters map: " + n);
                        }
                        reportsSet.clear();
                        // TODO: clear reportsPerHost[n]
                    }
                    final List<Node> ret = ImmutableList.copyOf(proposal);
                    proposal.clear();
                    return ret;
                }
            }

            return EMPTY_LIST;
        }
    }

    void printMetrics() {
        System.out.println("===============================");
        System.out.println(updatesInProgress);
        System.out.println(proposal);
        for (final Map.Entry<Node, Map<Integer, HostAndPort>> entry: reportsPerHost.entrySet()) {
            System.out.println(entry);
        }
        System.out.println("===============================");
    }


    static final class Node {
        final HostAndPort hostAndPort;
        final UUID uuid;

        Node(final HostAndPort hostAndPort, final UUID uuid) {
            this.hostAndPort = hostAndPort;
            this.uuid = uuid;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof Node) {
                final Node other = (Node) obj;
                return (this.hostAndPort.equals(other.hostAndPort) &&
                        this.uuid.equals(other.uuid));
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 31 * hostAndPort.hashCode() + uuid.hashCode();
        }

        @Override
        public String toString() {
            return "[" + hostAndPort.toString() + "," + uuid + "]";
        }
    }
}