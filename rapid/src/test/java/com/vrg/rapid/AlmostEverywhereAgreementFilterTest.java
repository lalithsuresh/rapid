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

import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.EdgeStatus;
import com.vrg.rapid.pb.AlertMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for almost-everywhere agreement
 */
public class AlmostEverywhereAgreementFilterTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 2;
    private static final long CONFIGURATION_ID = -1;  // Should not affect the following tests

    /**
     * A series of updates with the right ring indexes
     */
    @Test
    public void aeaFilterTest() {
        final AlmostEverywhereAgreementFilter wb = new AlmostEverywhereAgreementFilter(K, H, L);
        final Endpoint dst = Utils.hostFromParts("127.0.0.2", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(1, ret.size());
        assertEquals(1, wb.getNumProposals());
    }

    @Test
    public void aeaFilterTestBlockingOneBlocker() {
        final AlmostEverywhereAgreementFilter wb = new AlmostEverywhereAgreementFilter(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst2, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(2, ret.size());
        assertEquals(1, wb.getNumProposals());
    }


    @Test
    public void aeaFilterTestBlockingThreeBlockers() {
        final AlmostEverywhereAgreementFilter wb = new AlmostEverywhereAgreementFilter(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        final Endpoint dst3 = Utils.hostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst3, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst3, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst2, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(3, ret.size());
        assertEquals(1, wb.getNumProposals());
    }

    @Test
    public void aeaFilterTestBlockingMultipleBlockersPastH() {
        final AlmostEverywhereAgreementFilter wb = new AlmostEverywhereAgreementFilter(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        final Endpoint dst3 = Utils.hostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst3, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        // Unlike the previous test, add more reports for
        // dst1 and dst3 past the H boundary.
        wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H + 1), dst1, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst3, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H + 1), dst3, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());


        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst2, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(3, ret.size());
        assertEquals(1, wb.getNumProposals());
    }

    @Test
    public void aeaFilterTestBelowL() {
        final AlmostEverywhereAgreementFilter wb = new AlmostEverywhereAgreementFilter(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        final Endpoint dst3 = Utils.hostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        // Unlike the previous test, dst2 has < L updates
        for (int i = 0; i < L - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst3, EdgeStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createAlertMessage(
                Utils.hostFromParts("127.0.0.1", H), dst3, EdgeStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(2, ret.size());
        assertEquals(1, wb.getNumProposals());
    }


    @Test
    public void aeaFilterTestBatch() {
        final AlmostEverywhereAgreementFilter wb = new AlmostEverywhereAgreementFilter(K, H, L);
        final int numNodes = 3;
        final List<Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            endpoints.add(Utils.hostFromParts("127.0.0.2", 2 + i));
        }

        final List<Endpoint> proposal = new ArrayList<>();
        for (final Endpoint endpoint : endpoints) {
            for (int ringNumber = 0; ringNumber < K; ringNumber++) {
                proposal.addAll(wb.aggregateForProposal(createAlertMessage(
                        Utils.hostFromParts("127.0.0.1", 1), endpoint, EdgeStatus.UP,
                        CONFIGURATION_ID, ringNumber)));
            }
        }

        assertEquals(proposal.size(), numNodes);
    }

    @Test
    public void aeaFilterTestLinkInvalidation() {
        final MembershipView mView = new MembershipView(K);
        final AlmostEverywhereAgreementFilter wb = new AlmostEverywhereAgreementFilter(K, H, L);
        final int numNodes = 30;
        final List<Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            final Endpoint node = Utils.hostFromParts("127.0.0.2", 2 + i);
            endpoints.add(node);
            mView.ringAdd(node, Utils.nodeIdFromUUID(UUID.randomUUID()));
        }

        final Endpoint dst = endpoints.get(0);
        final List<Endpoint> observers = mView.getObserversOf(dst);
        assertEquals(K, observers.size());

        List<Endpoint> ret;

        // This adds alerts from the observers[0, H - 1) of node dst.
        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createAlertMessage(observers.get(i), dst,
                                                                  EdgeStatus.DOWN, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        // Next, we add alerts *about* observers[H, K) of node dst.
        final Set<Endpoint> failedObservers = new HashSet<>(K - H - 1);
        for (int i = H - 1; i < K; i++) {
            final List<Endpoint> observersOfObserver = mView.getObserversOf(observers.get(i));
            failedObservers.add(observers.get(i));
            for (int j = 0; j < K; j++) {
                ret = wb.aggregateForProposal(createAlertMessage(observersOfObserver.get(j), observers.get(i),
                        EdgeStatus.DOWN, CONFIGURATION_ID, j));
                assertEquals(0, ret.size());
                assertEquals(0, wb.getNumProposals());
            }
        }

        // At this point, (K - H - 1) observers of dst will be past H, and dst will be in H - 1. Link invalidation
        // should bring the failed observers and dst to the stable region.
        ret = wb.invalidateFailingEdges(mView);
        assertEquals(4, ret.size());
        assertEquals(1, wb.getNumProposals());
        for (final Endpoint node: ret) {
            assertTrue(failedObservers.contains(node) || node.equals(dst));
        }
    }

    private AlertMessage createAlertMessage(final Endpoint src,
                                                      final Endpoint dst,
                                                      final EdgeStatus status,
                                                      final long configuration,
                                                      final int ringNumber) {
        return AlertMessage.newBuilder()
                .setEdgeSrc(src)
                .setEdgeDst(dst)
                .setEdgeStatus(status)
                .addRingNumber(ringNumber)
                .setConfigurationId(configuration).build();
    }
}
