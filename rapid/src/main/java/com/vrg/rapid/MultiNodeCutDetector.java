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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.vrg.rapid.pb.AlertMessage;
import com.vrg.rapid.pb.EdgeStatus;
import com.vrg.rapid.pb.Endpoint;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A filter that outputs a view change proposal about a node only if:
 * - there are H reports about a node.
 * - there is no other node about which there are more than L but less than H reports.
 *
 * The output of this filter gives us almost-everywhere agreement
 */
final class MultiNodeCutDetector {
    private static final int K_MIN = 3;
    private static final int REINFORCE_TIMEOUT_DEFAULT_IN_SECONDS = 10;
    private final int K; // Number of observers per subject and vice versa
    private final int H; // High watermark
    private final int L; // Low watermark
    private final Object lock = new Object();
    @GuardedBy("lock") private int proposalCount = 0;
    @GuardedBy("lock") private int updatesInProgress = 0;
    @GuardedBy("lock") private final Map<Endpoint, Map<Integer, Endpoint>> reportsPerHost;
    @GuardedBy("lock") private final Set<Endpoint> proposal = new HashSet<>();
    @GuardedBy("lock") private final Set<Endpoint> preProposal = new HashSet<>();
    @GuardedBy("lock") private boolean seenLinkDownEvents = false;
    private final SharedResources resources;
    private final int reinforceTimeoutInSeconds;
    @GuardedBy("lock") private SettableFuture<Set<Endpoint>> proposalFuture;
    @GuardedBy("lock") @Nullable private ScheduledFuture<?> reinforceFuture = null;

    MultiNodeCutDetector(final int K, final int H, final int L, final SharedResources resources) {
        if (H > K || L > H || K < K_MIN || L <= 0 || H <= 0) {
            throw new IllegalArgumentException("Arguments do not satisfy K > H >= L >= 0:" +
                    " (K: " + K + ", H: " + H + ", L: " + L);
        }
        this.K = K;
        this.H = H;
        this.L = L;
        this.reportsPerHost = new HashMap<>();
        this.proposalFuture = SettableFuture.create();
        this.resources = resources;
        this.reinforceTimeoutInSeconds = REINFORCE_TIMEOUT_DEFAULT_IN_SECONDS;
    }

    MultiNodeCutDetector(final int K, final int H, final int L, final SharedResources resources,
                         final int reinforceTimeoutInSeconds) {
        if (H > K || L > H || K < K_MIN || L <= 0 || H <= 0) {
            throw new IllegalArgumentException("Arguments do not satisfy K > H >= L >= 0:" +
                    " (K: " + K + ", H: " + H + ", L: " + L);
        }
        this.K = K;
        this.H = H;
        this.L = L;
        this.reportsPerHost = new HashMap<>();
        this.proposalFuture = SettableFuture.create();
        this.resources = resources;
        this.reinforceTimeoutInSeconds = reinforceTimeoutInSeconds;
    }

    int getNumProposals() {
        synchronized (lock) {
            return proposalCount;
        }
    }


    /**
     * Apply a AlertMessage against the cut detector. When an update moves a host
     * past the H threshold of reports, and no other host has between H and L reports, the
     * method returns a view change proposal.
     *
     * @param msgs A list of AlertMessages to apply against the filter
     * @return a list of endpoints about which a view change has been recorded. Empty list if there is no proposal.
     */
    ListenableFuture<Set<Endpoint>> aggregateForProposal(final List<AlertMessage> msgs, final MembershipView view) {
        Objects.requireNonNull(msgs);
        synchronized (lock) {
            msgs.forEach(msg -> msg.getRingNumberList()
                    .forEach(ringNumber -> aggregateForProposal(msg.getEdgeSrc(),
                            msg.getEdgeDst(), msg.getEdgeStatus(), ringNumber)));
            // Apply implicit detections
            invalidateFailingEdges(view);
            if (updatesInProgress == 0 && !proposal.isEmpty()) {
                proposalFuture.set(ImmutableSet.copyOf(proposal));
            }
            return proposalFuture;
        }
    }

    /**
     * Apply a AlertMessage against the cut detector. When an update moves a host
     * past the H threshold of reports, and no other host has between H and L reports, the
     * method returns a view change proposal.
     *
     * @param msg A AlertMessage to apply against the filter
     * @return a list of endpoints about which a view change has been recorded. Empty list if there is no proposal.
     */
    ListenableFuture<Set<Endpoint>> aggregateForProposal(final AlertMessage msg, final MembershipView view) {
        return aggregateForProposal(Collections.singletonList(msg), view);
    }

    private void aggregateForProposal(final Endpoint linkSrc, final Endpoint linkDst,
                                                final EdgeStatus edgeStatus, final int ringNumber) {
        assert ringNumber <= K;

        synchronized (lock) {
            if (edgeStatus == EdgeStatus.DOWN) {
                seenLinkDownEvents = true;
            }

            final Map<Integer, Endpoint> reportsForHost = reportsPerHost.computeIfAbsent(
                                                                 linkDst,
                                                                 k -> new HashMap<>(K));

            if (reportsForHost.containsKey(ringNumber)) {
                return;  // duplicate announcement, ignore.
            }

            reportsForHost.put(ringNumber, linkSrc);
            final int numReportsForHost = reportsForHost.size();

            if (numReportsForHost == L) {
                updatesInProgress++;
                preProposal.add(linkDst);
                if (reinforceFuture == null) {
                    reinforceFuture = resources.getScheduledTasksExecutor().schedule(this::reinforce,
                            reinforceTimeoutInSeconds, TimeUnit.SECONDS);
                }
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
                }
            }
        }
    }

    /**
     * Invalidates edges between nodes that are failing or have failed. This step may be skipped safely
     * when there are no failing nodes.
     *
     * @param view MembershipView object required to find observer-subject relationships between failing nodes.
     */
    private void invalidateFailingEdges(final MembershipView view) {
        synchronized (lock) {
            // Link invalidation is only required when we have failing nodes
            if (!seenLinkDownEvents) {
                return;
            }

            // We iterate on a copy of this.preProposal because aggregateForProposal() modifies it.
            final List<Endpoint> preProposalCopy = ImmutableList.copyOf(preProposal);
            for (final Endpoint nodeInFlux: preProposalCopy) {
                final List<Endpoint> observers = view.isHostPresent(nodeInFlux)
                                                    ? view.getObserversOf(nodeInFlux)          // For failing nodes
                                                    : view.getExpectedObserversOf(nodeInFlux); // For joining nodes
                // Account for all edges between nodes that are past the L threshold
                int ringNumber = 0;
                for (final Endpoint observer : observers) {
                    if (proposal.contains(observer) || preProposal.contains(observer)) {
                        // Implicit detection of edges between observer and nodeInFlux
                        final EdgeStatus edgeStatus = view.isHostPresent(nodeInFlux) ? EdgeStatus.DOWN : EdgeStatus.UP;
                        aggregateForProposal(observer, nodeInFlux, edgeStatus, ringNumber);
                    }
                    ringNumber++;
                }
            }
        }
    }

    private void reinforce() {
        synchronized (lock) {
            final Set<Endpoint> result = new HashSet<>(preProposal.size() + proposal.size());
            result.addAll(preProposal);
            result.addAll(proposal);
            proposalFuture.set(result);
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
            proposalFuture = SettableFuture.create();
            reinforceFuture = null;
        }
    }
}