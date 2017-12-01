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
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.LinkUpdateMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for a watermark-buffer
 */
public class WatermarkBufferTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 2;
    private static final long CONFIGURATION_ID = -1;  // Should not affect the following tests

    /**
     * A series of updates with the right ring indexes
     */
    @Test
    public void waterMarkTest() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final Endpoint dst = Utils.hostFromParts("127.0.0.2", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(1, ret.size());
        assertEquals(1, wb.getNumProposals());
    }

    @Test
    public void waterMarkTestBlockingOneBlocker() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst2, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(2, ret.size());
        assertEquals(1, wb.getNumProposals());
    }


    @Test
    public void waterMarkTestBlockingThreeBlockers() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        final Endpoint dst3 = Utils.hostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst3, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst3, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst2, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(3, ret.size());
        assertEquals(1, wb.getNumProposals());
    }

    @Test
    public void waterMarkTestBlockingMultipleBlockersPastH() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        final Endpoint dst3 = Utils.hostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst3, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        // Unlike the previous test, add more reports for
        // dst1 and dst3 past the H boundary.
        wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H + 1), dst1, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst3, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H + 1), dst3, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());


        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst2, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(3, ret.size());
        assertEquals(1, wb.getNumProposals());
    }

    @Test
    public void waterMarkTestBelowL() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final Endpoint dst1 = Utils.hostFromParts("127.0.0.2", 2);
        final Endpoint dst2 = Utils.hostFromParts("127.0.0.3", 2);
        final Endpoint dst3 = Utils.hostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst1, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        // Unlike the previous test, dst2 has < L updates
        for (int i = 0; i < L - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst2, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(
                    Utils.hostFromParts("127.0.0.1", i + 1), dst3, LinkStatus.UP, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst1, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumProposals());

        ret = wb.aggregateForProposal(createLinkUpdateMessage(
                Utils.hostFromParts("127.0.0.1", H), dst3, LinkStatus.UP, CONFIGURATION_ID, H - 1));
        assertEquals(2, ret.size());
        assertEquals(1, wb.getNumProposals());
    }


    @Test
    public void waterMarkTestBatch() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final int numNodes = 3;
        final List<Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            endpoints.add(Utils.hostFromParts("127.0.0.2", 2 + i));
        }

        final List<Endpoint> proposal = new ArrayList<>();
        for (final Endpoint endpoint : endpoints) {
            for (int ringNumber = 0; ringNumber < K; ringNumber++) {
                proposal.addAll(wb.aggregateForProposal(createLinkUpdateMessage(
                        Utils.hostFromParts("127.0.0.1", 1), endpoint, LinkStatus.UP,
                        CONFIGURATION_ID, ringNumber)));
            }
        }

        assertEquals(proposal.size(), numNodes);
    }

    @Test
    public void waterMarkTestLinkInvalidation() {
        final MembershipView mView = new MembershipView(K);
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final int numNodes = 30;
        final List<Endpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            final Endpoint node = Utils.hostFromParts("127.0.0.2", 2 + i);
            endpoints.add(node);
            mView.ringAdd(node, Utils.nodeIdFromUUID(UUID.randomUUID()));
        }

        final Endpoint dst = endpoints.get(0);
        final List<Endpoint> monitors = mView.getMonitorsOf(dst);
        assertEquals(K, monitors.size());

        List<Endpoint> ret;

        // This adds alerts from the monitors[0, H - 1) of node dst.
        for (int i = 0; i < H - 1; i++) {
            ret = wb.aggregateForProposal(createLinkUpdateMessage(monitors.get(i), dst,
                                                                  LinkStatus.DOWN, CONFIGURATION_ID, i));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumProposals());
        }

        // Next, we add alerts *about* monitors[H, K) of node dst.
        final Set<Endpoint> failedMonitors = new HashSet<>(K - H - 1);
        for (int i = H - 1; i < K; i++) {
            final List<Endpoint> monitorsOfMonitor = mView.getMonitorsOf(monitors.get(i));
            failedMonitors.add(monitors.get(i));
            for (int j = 0; j < K; j++) {
                ret = wb.aggregateForProposal(createLinkUpdateMessage(monitorsOfMonitor.get(j), monitors.get(i),
                        LinkStatus.DOWN, CONFIGURATION_ID, j));
                assertEquals(0, ret.size());
                assertEquals(0, wb.getNumProposals());
            }
        }

        // At this point, (K - H - 1) monitors of dst will be past H, and dst will be in H - 1. Link invalidation
        // should bring the failed monitors and dst to the stable region.
        ret = wb.invalidateFailingLinks(mView);
        assertEquals(4, ret.size());
        assertEquals(1, wb.getNumProposals());
        for (final Endpoint node: ret) {
            assertTrue(failedMonitors.contains(node) || node.equals(dst));
        }
    }

    private LinkUpdateMessage createLinkUpdateMessage(final Endpoint src,
                                                      final Endpoint dst,
                                                      final LinkStatus status,
                                                      final long configuration,
                                                      final int ringNumber) {
        return LinkUpdateMessage.newBuilder()
                .setLinkSrc(src)
                .setLinkDst(dst)
                .setLinkStatus(status)
                .addRingNumber(ringNumber)
                .setConfigurationId(configuration).build();
    }
}
