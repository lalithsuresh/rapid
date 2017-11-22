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

import com.google.common.collect.ImmutableSet;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.NodeId;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for a standalone MembershipView object (no watermark buffer).
 */
public class MembershipViewTest {
    private static final int K = 10;

    /**
     * Add a single node and verify whether it appears on all rings
     */
    @Test
    public void oneRingAddition() {
        final MembershipView mview = new MembershipView(K);
        final Endpoint addr = Utils.hostFromParts("127.0.0.1", 123);
        try {
            mview.ringAdd(addr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        } catch (final MembershipView.NodeAlreadyInRingException e) {
            fail();
        }

        for (int k = 0; k < K; k++) {
            final List<Endpoint> list = mview.getRing(k);
            assertEquals(1, list.size());
            for (final Endpoint address : list) {
                assertEquals(address, addr);
            }
        }
    }

    /**
     * Add multiple nodes and verify whether they appears on all rings
     */
    @Test
    public void multipleRingAdditions() {
        final MembershipView mview = new MembershipView(K);
        final int numNodes = 10;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(Utils.hostFromParts("127.0.0.1", i), Utils.nodeIdFromUUID(UUID.randomUUID()));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }
        for (int k = 0; k < K; k++) {
            final List<Endpoint> list = mview.getRing(k);
            assertEquals(numNodes, list.size());
        }
    }

    /**
     * Add multiple nodes twice and verify whether the rings rejects duplicates
     */
    @Test
    public void ringReAdditions() {
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        final int startPort = 0;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(Utils.hostFromParts("127.0.0.1", startPort + i),
                              Utils.nodeIdFromUUID(UUID.randomUUID()));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int k = 0; k < K; k++) {
            final List<Endpoint> list = mview.getRing(k);
            assertEquals(numNodes, list.size());
        }

        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(Utils.hostFromParts("127.0.0.1", startPort + i),
                              Utils.nodeIdFromUUID(UUID.randomUUID()));
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
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(Utils.hostFromParts("127.0.0.1", i));
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
        final MembershipView mview = new MembershipView(K);
        final int numNodes = 10;

        for (int i = 0; i < numNodes; i++) {
            mview.ringAdd(Utils.hostFromParts("127.0.0.1", i), Utils.nodeIdFromUUID(UUID.randomUUID()));
        }

        for (int i = 0; i < numNodes; i++) {
            mview.ringDelete(Utils.hostFromParts("127.0.0.1", i));
        }

        for (int k = 0; k < K; k++) {
            final List<Endpoint> list = mview.getRing(k);
            assertEquals(0, list.size());
        }
    }

    /**
     * Verify the edge case of monitoring relationships in a single node case.
     */
    @Test
    public void monitoringRelationshipEdge() {
        final MembershipView mview = new MembershipView(K);

        try {
            final Endpoint n1 = Utils.hostFromParts("127.0.0.1", 1);
            mview.ringAdd(n1, Utils.nodeIdFromUUID(UUID.randomUUID()));
            assertEquals(0, mview.getMonitoreesOf(n1).size());
            assertEquals(0, mview.getMonitorsOf(n1).size());
        } catch (final MembershipView.NodeAlreadyInRingException | MembershipView.NodeNotInRingException e) {
            fail();
        }

        final Endpoint n2 = Utils.hostFromParts("127.0.0.1", 2);

        try {
            mview.getMonitoreesOf(n2);
            fail();
        } catch (final MembershipView.NodeNotInRingException ignored) {
        }

        try {
            mview.getMonitorsOf(n2);
            fail();
        } catch (final MembershipView.NodeNotInRingException ignored) {
        }
    }

    /**
     * Verify the edge case of monitoring relationships in an empty view case.
     */
    @Test
    public void monitoringRelationshipEmpty() {
        int numExceptions = 0;
        final MembershipView mview = new MembershipView(K);
        final Endpoint n = Utils.hostFromParts("127.0.0.1", 1);

        try {
            mview.getMonitoreesOf(n);
        } catch (final MembershipView.NodeNotInRingException e) {
            numExceptions++;
        }
        assertEquals(1, numExceptions);

        try {
            mview.getMonitorsOf(n);
        } catch (final MembershipView.NodeNotInRingException e) {
            numExceptions++;
        }
        assertEquals(2, numExceptions);
    }

    /**
     * Verify the monitoring relationships in a two node setting
     */
    @Test
    public void monitoringRelationshipTwoNodes() {
        try {
            final MembershipView mview = new MembershipView(K);
            final Endpoint n1 = Utils.hostFromParts("127.0.0.1", 1);
            final Endpoint n2 = Utils.hostFromParts("127.0.0.1", 2);
            mview.ringAdd(n1, Utils.nodeIdFromUUID(UUID.randomUUID()));
            mview.ringAdd(n2, Utils.nodeIdFromUUID(UUID.randomUUID()));
            assertEquals(K, mview.getMonitoreesOf(n1).size());
            assertEquals(K, mview.getMonitorsOf(n1).size());
            assertEquals(1, ImmutableSet.copyOf(mview.getMonitoreesOf(n1)).size());
            assertEquals(1, ImmutableSet.copyOf(mview.getMonitorsOf(n1)).size());
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
            final MembershipView mview = new MembershipView(K);
            final Endpoint n1 = Utils.hostFromParts("127.0.0.1", 1);
            final Endpoint n2 = Utils.hostFromParts("127.0.0.1", 2);
            final Endpoint n3 = Utils.hostFromParts("127.0.0.1", 3);
            mview.ringAdd(n1, Utils.nodeIdFromUUID(UUID.randomUUID()));
            mview.ringAdd(n2, Utils.nodeIdFromUUID(UUID.randomUUID()));
            mview.ringAdd(n3, Utils.nodeIdFromUUID(UUID.randomUUID()));
            assertEquals(K, mview.getMonitoreesOf(n1).size());
            assertEquals(K, mview.getMonitorsOf(n1).size());
            assertEquals(2, ImmutableSet.copyOf(mview.getMonitoreesOf(n1)).size());
            assertEquals(2, ImmutableSet.copyOf(mview.getMonitorsOf(n1)).size());
            mview.ringDelete(n2);
            assertEquals(K, mview.getMonitoreesOf(n1).size());
            assertEquals(K, mview.getMonitorsOf(n1).size());
            assertEquals(1, ImmutableSet.copyOf(mview.getMonitoreesOf(n1)).size());
            assertEquals(1, ImmutableSet.copyOf(mview.getMonitorsOf(n1)).size());
        } catch (final MembershipView.NodeAlreadyInRingException | MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    /**
     * Verify the monitoring relationships in a multi node setting.
     */
    @Test
    public void monitoringRelationshipMultipleNodes() {
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 1000;
        final ArrayList<Endpoint> list = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            final Endpoint n = Utils.hostFromParts("127.0.0.1", i);
            list.add(n);
            try {
                mview.ringAdd(n, Utils.nodeIdFromUUID(UUID.randomUUID()));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int i = 0; i < numNodes; i++) {
            try {
                final int numMonitorees = mview.getMonitoreesOf(list.get(i)).size();
                final int numMonitors = mview.getMonitorsOf(list.get(i)).size();
                assertTrue("NumMonitorees: " + numMonitorees, K == numMonitorees);
                assertTrue("NumMonitors: " + numMonitors, K == numMonitors);
            } catch (final MembershipView.NodeNotInRingException e) {
                fail();
            }
        }
    }

    /**
     * Verify the monitoring relationships during bootstrap.
     */
    @Test
    public void monitoringRelationshipBootstrap() {
        final MembershipView mview = new MembershipView(K);
        final int serverPort = 1234;
        final Endpoint n = Utils.hostFromParts("127.0.0.1", serverPort);
        try {
            mview.ringAdd(n, Utils.nodeIdFromUUID(UUID.randomUUID()));
        } catch (final MembershipView.NodeAlreadyInRingException e) {
            fail();
        }

        final Endpoint joiningNode = Utils.hostFromParts("127.0.0.1", serverPort + 1);
        assertEquals(K, mview.getExpectedMonitorsOf(joiningNode).size());
        assertEquals(1, ImmutableSet.copyOf(mview.getExpectedMonitorsOf(joiningNode)).size());
        assertEquals(n, mview.getExpectedMonitorsOf(joiningNode).toArray()[0]);
    }

    /**
     * Verify the monitoring relationships during bootstrap with up to K nodes
     */
    @Test
    public void monitoringRelationshipBootstrapMultiple() {
        final MembershipView mview = new MembershipView(K);
        final int numNodes = 20;
        final int serverPortBase = 1234;
        final Endpoint joiningNode = Utils.hostFromParts("127.0.0.1", serverPortBase - 1);
        int numMonitors = 0;
        for (int i = 0; i < numNodes; i++) {
            final Endpoint n = Utils.hostFromParts("127.0.0.1", serverPortBase + i);
            try {
                mview.ringAdd(n, Utils.nodeIdFromUUID(UUID.randomUUID()));
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }

            final int numMonitorsActual = mview.getExpectedMonitorsOf(joiningNode).size();

            // we could compare against i + 1 but that condition is not guaranteed
            // to hold true since we are not constructing deterministic expanders
            assertTrue(numMonitors <= numMonitorsActual);
            numMonitors = numMonitorsActual;
        }

        // See if we have roughly K monitors.
        assertTrue(K - 3 <= numMonitors);
        assertTrue(K >= numMonitors);
    }


    /**
     * Test for different combinations of a host joining with a unique ID
     */
    @Test
    public void nodeUniqueIdNoDeletions() {
        final MembershipView mview = new MembershipView(K);
        int numExceptions = 0;
        final Endpoint n1 = Utils.hostFromParts("127.0.0.1", 1);
        final NodeId id1 = Utils.nodeIdFromUUID(UUID.randomUUID());
        mview.ringAdd(n1, id1);

        final Endpoint n2 = Utils.hostFromParts("127.0.0.1", 1);
        final NodeId id2 = NodeId.newBuilder(id1).build();

        // Same host, same ID
        try {
            mview.ringAdd(n2, id2);
        } catch (final MembershipView.UUIDAlreadySeenException e) {
            numExceptions++;
        }
        assertEquals(1, numExceptions);

        // Same host, different ID
        try {
            mview.ringAdd(n2, Utils.nodeIdFromUUID(UUID.randomUUID()));
        } catch (final MembershipView.NodeAlreadyInRingException e) {
            numExceptions++;
        }
        assertEquals(2, numExceptions);

        // different host, same ID
        final Endpoint n3 = Utils.hostFromParts("127.0.0.1", 2);
        try {
            mview.ringAdd(n3, id2);
        } catch (final MembershipView.UUIDAlreadySeenException e) {
            numExceptions++;
        }
        assertEquals(3, numExceptions);

        // different host, different ID
        try {
            mview.ringAdd(n3, Utils.nodeIdFromUUID(UUID.randomUUID()));
        } catch (final MembershipView.NodeNotInRingException e) {
            numExceptions++;
        }
        // Should not have triggered an exception
        assertEquals(3, numExceptions);

        // Only n1 and n3 should have been added
        assertEquals(2, mview.getRing(0).size());
    }


    /**
     * Test for different combinations of a host and unique ID
     * after it was removed
     */
    @Test
    public void nodeUniqueIdWithDeletions() {
        final MembershipView mview = new MembershipView(K);

        final Endpoint n1 = Utils.hostFromParts("127.0.0.1", 1);
        final NodeId id1 = Utils.nodeIdFromUUID(UUID.randomUUID());
        mview.ringAdd(n1, id1);

        final Endpoint n2 = Utils.hostFromParts("127.0.0.1", 2);
        final NodeId id2 = Utils.nodeIdFromUUID(UUID.randomUUID());

        // Same host, same ID
        mview.ringAdd(n2, id2);

        // Node is removed from the ring
        mview.ringDelete(n2);
        assertEquals(1, mview.getRing(0).size());

        int numExceptions = 0;
        // Node rejoins with id2
        try {
            mview.ringAdd(n2, id2);
        } catch (final MembershipView.UUIDAlreadySeenException e) {
            numExceptions++;
        }
        assertEquals(1, numExceptions);

        // Re-attempt with new ID
        mview.ringAdd(n2, Utils.nodeIdFromUUID(UUID.randomUUID()));
        assertEquals(2, mview.getRing(0).size());
    }


    /**
     * Ensure that N different configuration IDs are generated
     * when N nodes are added to the rings
     */
    @Test
    public void nodeConfigurationChange() {
        final MembershipView mview = new MembershipView(K);
        final int numNodes = 1000;
        final Set<Long> set = new HashSet<>(numNodes);

        for (int i = 0; i < numNodes; i++) {
            final Endpoint n = Utils.hostFromParts("127.0.0.1", i);
            mview.ringAdd(n, Utils.nodeIdFromUUID(
                                UUID.nameUUIDFromBytes(n.toString().getBytes(StandardCharsets.UTF_8))
                             ));
            set.add(mview.getCurrentConfigurationId());
        }
        assertEquals(numNodes, set.size()); // should be 1000 different configurations
    }


    /**
     * Add endpoints to the two membership view objects in different
     * orders. All except the last generated configuration identifier
     * should be different.
     */
    @Test
    public void nodeConfigurationsAcrossMViews() {
        final MembershipView mview1 = new MembershipView(K);
        final MembershipView mview2 = new MembershipView(K);
        final int numNodes = 1000;
        final List<Long> list1 = new ArrayList<>(numNodes);
        final List<Long> list2 = new ArrayList<>(numNodes);


        for (int i = 0; i < numNodes; i++) {
            final Endpoint n = Utils.hostFromParts("127.0.0.1", i);
            mview1.ringAdd(n, Utils.nodeIdFromUUID(
                                UUID.nameUUIDFromBytes(n.toString().getBytes(StandardCharsets.UTF_8))
                              ));
            list1.add(mview1.getCurrentConfigurationId());
        }

        for (int i = numNodes - 1; i > -1; i--) {
            final Endpoint n = Utils.hostFromParts("127.0.0.1", i);
            mview2.ringAdd(n, Utils.nodeIdFromUUID(
                                UUID.nameUUIDFromBytes(n.toString().getBytes(StandardCharsets.UTF_8))
                              ));
            list2.add(mview2.getCurrentConfigurationId());
        }

        assertEquals(numNodes, list1.size());
        assertEquals(numNodes, list2.size());

        // Only the last added elements in the sequence
        // of configurations should have the same value
        final Iterator<Long> iter1 = list1.iterator();
        final Iterator<Long> iter2 = list2.iterator();
        for (int i = 0; i < numNodes - 1; i++) {
            assertNotEquals(iter1.next(), iter2.next());
        }
        assertEquals(iter1.next(), iter2.next());
    }
}