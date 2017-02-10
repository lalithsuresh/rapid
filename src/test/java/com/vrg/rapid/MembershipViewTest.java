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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for a standalone MembershipView object (no watermark buffer).
 */
public class MembershipViewTest {

    /**
     * Add a single node and verify whether it appears on all rings
     */
    @Test
    public void oneRingAddition() {
        final int K = 10;
        final MembershipView mview = new MembershipView(K);
        final HostAndPort addr = HostAndPort.fromParts("127.0.0.1", 123);
        final Node node = new Node(addr);
        try {
            mview.ringAdd(node);
        } catch (final MembershipView.NodeAlreadyInRingException e) {
            fail();
        }

        for (int k = 0; k < K; k++) {
            final List<Node> list = mview.viewRing(k);
            assertEquals(1, list.size());
            for (final Node n : list) {
                assertEquals(n.address, addr);
            }
        }
    }

    /**
     * Add multiple nodes and verify whether they appears on all rings
     */
    @Test
    public void multipleRingAdditions() {
        final int K = 10;
        final MembershipView mview = new MembershipView(K);
        final int numNodes = 10;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(HostAndPort.fromParts("127.0.0.1", i)));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }
        for (int k = 0; k < K; k++) {
            final List<Node> list = mview.viewRing(k);
            assertEquals(numNodes, list.size());
        }
    }

    /**
     * Add multiple nodes twice and verify whether the rings rejects duplicates
     */
    @Test
    public void ringReAdditions() {
        final int K = 10;
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        final int startPort = 0;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(HostAndPort.fromParts("127.0.0.1", startPort + i)));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int k = 0; k < K; k++) {
            final List<Node> list = mview.viewRing(k);
            assertEquals(numNodes, list.size());
        }

        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(HostAndPort.fromParts("127.0.0.1", startPort + i)));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                numThrows++;
            }
        }

        assertEquals(numNodes, numThrows);
    }

    /**
     * Delete nodes that were never added and verify whether the object rejects those attempts
     */
    @Test
    public void ringDeletionsOnly() {
        final int K = 10;
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(new Node(HostAndPort.fromParts("127.0.0.1", i)));
            } catch (final MembershipView.NodeNotInRingException e) {
                numThrows++;
            }
        }

        assertEquals(numNodes, numThrows);
    }

    /**
     * Add nodes and then delete them.
     */
    @Test
    public void ringAdditionsAndDeletions() {
        final int K = 10;
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        int numThrows = 0;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(HostAndPort.fromParts("127.0.0.1", i)));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(new Node(HostAndPort.fromParts("127.0.0.1", i)));
            } catch (final MembershipView.NodeNotInRingException e) {
                numThrows++;
            }
        }

        assertEquals(0, numThrows);

        for (int k = 0; k < K; k++) {
            final List<Node> list = mview.viewRing(k);
            assertEquals(0, list.size());
        }
    }

    /**
     * Verify the edge case of monitoring relationships in a single node case.
     */
    @Test
    public void monitoringRelationshipEdge() {
        try {
            final int K = 10;
            final MembershipView mview = new MembershipView(K);
            final Node n1 = new Node(HostAndPort.fromParts("127.0.0.1", 1));
            mview.ringAdd(n1);
            assertEquals(0, mview.monitoreesOf(n1).size());
            assertEquals(0, mview.monitorsOf(n1).size());

            final Node n2 = new Node(HostAndPort.fromParts("127.0.0.1", 2));
            assertEquals(0, mview.monitoreesOf(n2).size());
            assertEquals(0, mview.monitorsOf(n2).size());
        } catch (final MembershipView.NodeAlreadyInRingException | MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    /**
     * Verify the edge case of monitoring relationships in an empty view case.
     */
    @Test
    public void monitoringRelationshipEmpty() {
        try {
            final int K = 10;
            final MembershipView mview = new MembershipView(K);
            final Node n = new Node(HostAndPort.fromParts("127.0.0.1", 1));
            assertEquals(0, mview.monitoreesOf(n).size());
            assertEquals(0, mview.monitorsOf(n).size());
        } catch (final MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    /**
     * Verify the monitoring relationships in a two node setting
     */
    @Test
    public void monitoringRelationshipTwoNodes() {
        try {
            final int K = 10;
            final MembershipView mview = new MembershipView(K);
            final Node n1 = new Node(HostAndPort.fromParts("127.0.0.1", 1));
            final Node n2 = new Node(HostAndPort.fromParts("127.0.0.1", 2));
            mview.ringAdd(n1);
            mview.ringAdd(n2);
            assertEquals(1, mview.monitoreesOf(n1).size());
            assertEquals(1, mview.monitorsOf(n1).size());
        } catch (final MembershipView.NodeAlreadyInRingException | MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    /**
     * Verify the monitoring relationships in a three node setting
     */
    @Test
    public void monitoringRelationshipThreeNodesWithDelete() {
        try {
            final int K = 10;
            final MembershipView mview = new MembershipView(K);
            final Node n1 = new Node(HostAndPort.fromParts("127.0.0.1", 1));
            final Node n2 = new Node(HostAndPort.fromParts("127.0.0.1", 2));
            final Node n3 = new Node(HostAndPort.fromParts("127.0.0.1", 3));
            mview.ringAdd(n1);
            mview.ringAdd(n2);
            mview.ringAdd(n3);
            assertEquals(2, mview.monitoreesOf(n1).size());
            assertEquals(2, mview.monitorsOf(n1).size());
            mview.ringDelete(n2);
            assertEquals(1, mview.monitoreesOf(n1).size());
            assertEquals(1, mview.monitorsOf(n1).size());
        } catch (final MembershipView.NodeAlreadyInRingException | MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    /**
     * Verify the monitoring relationships in a multi node setting.
     */
    @Test
    public void monitoringRelationshipMultipleNodes() {
        final int K = 10;
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 10000;
        final ArrayList<Node> list = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            final Node n = new Node(HostAndPort.fromParts("127.0.0.1", i));
            list.add(n);
            try {
                mview.ringAdd(n);
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int i = 0; i < numNodes; i++) {
            try {
                final int numMonitorees = mview.monitoreesOf(list.get(i)).size();
                final int numMonitors = mview.monitoreesOf(list.get(i)).size();
                assertTrue("NumMonitorees: " + numMonitorees, K - 3 <= numMonitorees);
                assertTrue("NumMonitorees: " + numMonitorees, K >= numMonitorees);
                assertTrue("NumMonitors: " + numMonitors, K - 3 <= numMonitors);
                assertTrue("NumMonitors: " + numMonitors, K >= numMonitors);
            } catch (final MembershipView.NodeNotInRingException e) {
                fail();
            }
        }
    }
}