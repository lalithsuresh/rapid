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
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
    @GuardedBy("lock") private int proposalCount = 0;
    @GuardedBy("lock") private int updatesInProgress = 0;
    @GuardedBy("lock") private final Map<Endpoint, Map<Integer, Endpoint>> reportsPerHost;
    @GuardedBy("lock") private final ArrayList<Endpoint> proposal = new ArrayList<>();
    @GuardedBy("lock") private final Set<Endpoint> preProposal = new HashSet<>();
    @GuardedBy("lock") private boolean seenLinkDownEvents = false;
    private final Object lock = new Object();

    WatermarkBuffer(final int K, final int H, final int L) {
        if (H > K || L > H || K < K_MIN || L <= 0 || H <= 0) {
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
            return proposalCount;
        }
    }

    /**
     * Apply a LinkUpdateMessage against the Watermark filter. When an update moves a host
     * past the H threshold of reports, and no other host has between H and L reports, the
     * method returns a view change proposal.
     *
     * @param msg A LinkUpdateMessage to apply against the filter
     * @return a list of endpoints about which a view change has been recorded. Empty list if there is no proposal.
     */
    List<Endpoint> aggregateForProposal(final LinkUpdateMessage msg) {
        Objects.requireNonNull(msg);
        final ArrayList<Endpoint> proposals = new ArrayList<>();
        msg.getRingNumberList().forEach(ringNumber ->
           proposals.addAll(aggregateForProposal(msg.getLinkSrc(), msg.getLinkDst(), msg.getLinkStatus(), ringNumber)));
        return proposals;
    }

    private List<Endpoint> aggregateForProposal(final Endpoint linkSrc, final Endpoint linkDst,
                                                final LinkStatus linkStatus, final int ringNumber) {
        assert ringNumber <= K;

        synchronized (lock) {
            if (linkStatus == LinkStatus.DOWN) {
                seenLinkDownEvents = true;
            }

            final Map<Integer, Endpoint> reportsForHost = reportsPerHost.computeIfAbsent(
                                                                 linkDst,
                                                                 k -> new HashMap<>(K));

            if (reportsForHost.containsKey(ringNumber)) {
                return Collections.emptyList();  // duplicate announcement, ignore.
            }

            reportsForHost.put(ringNumber, linkSrc);
            final int numReportsForHost = reportsForHost.size();

            if (numReportsForHost == L) {
                updatesInProgress++;
                preProposal.add(linkDst);
            }

            if (numReportsForHost == H) {
                // Enough reports about linkDst have been received that it is safe to act upon,
                // provided there are no other nodes with L < #reports < H.
                preProposal.remove(linkDst);
                proposal.add(linkDst);
                updatesInProgress--;

                if (updatesInProgress == 0) {
                    // No outstanding updates, so all nodes that have crossed the H threshold of reports are
                    // now part of a single proposal.
                    proposalCount++;
                    final List<Endpoint> ret = ImmutableList.copyOf(proposal);
                    proposal.clear();
                    return ret;
                }
            }

            return Collections.emptyList();
        }
    }

    /**
     * Invalidates links between nodes that are failing or have failed. This step may be skipped safely
     * when there are no failing nodes.
     *
     * @param view MembershipView object required to find monitor-monitoree relationships between failing nodes.
     * @return A list of endpoints representing a view change proposal.
     */
    List<Endpoint> invalidateFailingLinks(final MembershipView view) {
        synchronized (lock) {
            // Link invalidation is only required when we have failing nodes
            if (!seenLinkDownEvents) {
                return Collections.emptyList();
            }

            final List<Endpoint> proposalsToReturn = new ArrayList<>();
            final List<Endpoint> preProposalCopy = ImmutableList.copyOf(preProposal);
            for (final Endpoint nodeInFlux: preProposalCopy) {
                final List<Endpoint> monitors = view.isHostPresent(nodeInFlux)
                                                    ? view.getMonitorsOf(nodeInFlux)          // For failing nodes
                                                    : view.getExpectedMonitorsOf(nodeInFlux); // For joining nodes
                // Account for all links between nodes that are past the L threshold
                int ringNumber = 0;
                for (final Endpoint monitor : monitors) {
                    if (proposal.contains(monitor) || preProposal.contains(monitor)) {
                        // Implicit detection of link between monitor and nodeInFlux
                        final LinkStatus linkStatus = view.isHostPresent(nodeInFlux) ? LinkStatus.DOWN : LinkStatus.UP;
                        proposalsToReturn.addAll(aggregateForProposal(monitor, nodeInFlux, linkStatus, ringNumber));
                    }
                    ringNumber++;
                }
            }

            return ImmutableList.copyOf(proposalsToReturn);
        }
    }

    /**
     * Clears all view change reports being tracked. To be used right after a view change.
     */
    void clear() {
        synchronized (lock) {
            reportsPerHost.clear();
            proposal.clear();
            updatesInProgress = 0;
            proposalCount = 0;
            preProposal.clear();
            seenLinkDownEvents = false;
        }
    }
}