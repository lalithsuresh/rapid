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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
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
        final HostAndPort addr = HostAndPort.fromParts("127.0.0.1", 123);
        try {
            mview.ringAdd(addr, UUID.randomUUID());
        } catch (final MembershipView.NodeAlreadyInRingException e) {
            fail();
        }

        for (int k = 0; k < K; k++) {
            final List<HostAndPort> list = mview.viewRing(k);
            assertEquals(1, list.size());
            for (final HostAndPort address : list) {
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
                mview.ringAdd(HostAndPort.fromParts("127.0.0.1", i), UUID.randomUUID());
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }
        for (int k = 0; k < K; k++) {
            final List<HostAndPort> list = mview.viewRing(k);
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
                mview.ringAdd(HostAndPort.fromParts("127.0.0.1", startPort + i), UUID.randomUUID());
            } catch (final MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int k = 0; k < K; k++) {
            final List<HostAndPort> list = mview.viewRing(k);
            assertEquals(numNodes, list.size());
        }

        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(HostAndPort.fromParts("127.0.0.1", startPort + i), UUID.randomUUID());
            } catch (final Exception e) {
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
                mview.ringDelete(HostAndPort.fromParts("127.0.0.1", i));
            } catch (final Exception e) {
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
        int numThrows = 0;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(HostAndPort.fromParts("127.0.0.1", i), UUID.randomUUID());
            } catch (final Exception e) {
                fail();
            }
        }

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(HostAndPort.fromParts("127.0.0.1", i));
            } catch (final Exception e) {
                numThrows++;
            }
        }

        assertEquals(0, numThrows);

        for (int k = 0; k < K; k++) {
            final List<HostAndPort> list = mview.viewRing(k);
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
            final HostAndPort n1 = HostAndPort.fromParts("127.0.0.1", 1);
            mview.ringAdd(n1, UUID.randomUUID());
            assertEquals(0, mview.monitoreesOf(n1).size());
            assertEquals(0, mview.monitorsOf(n1).size());
        } catch (final MembershipView.NodeAlreadyInRingException | MembershipView.NodeNotInRingException e) {
            fail();
        }

        final HostAndPort n2 = HostAndPort.fromParts("127.0.0.1", 2);

        try {
            mview.monitoreesOf(n2).size();
            fail();
        } catch (final MembershipView.NodeNotInRingException ignored) {
        }

        try {
            mview.monitorsOf(n2).size();
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
        final HostAndPort n = HostAndPort.fromParts("127.0.0.1", 1);

        try {
            mview.monitoreesOf(n).size();
        } catch (final MembershipView.NodeNotInRingException e) {
            numExceptions++;
        }
        assertEquals(1, numExceptions);

        try {
            mview.monitorsOf(n).size();
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
            final HostAndPort n1 = HostAndPort.fromParts("127.0.0.1", 1);
            final HostAndPort n2 = HostAndPort.fromParts("127.0.0.1", 2);
            mview.ringAdd(n1, UUID.randomUUID());
            mview.ringAdd(n2, UUID.randomUUID());
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
            final MembershipView mview = new MembershipView(K);
            final HostAndPort n1 = HostAndPort.fromParts("127.0.0.1", 1);
            final HostAndPort n2 = HostAndPort.fromParts("127.0.0.1", 2);
            final HostAndPort n3 = HostAndPort.fromParts("127.0.0.1", 3);
            mview.ringAdd(n1, UUID.randomUUID());
            mview.ringAdd(n2, UUID.randomUUID());
            mview.ringAdd(n3, UUID.randomUUID());
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
        final MembershipView mview = new MembershipView(K);

        final int numNodes = 1000;
        final ArrayList<HostAndPort> list = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            final HostAndPort n = HostAndPort.fromParts("127.0.0.1", i);
            list.add(n);
            try {
                mview.ringAdd(n, UUID.randomUUID());
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

    /**
     * Test for different combinations of a host joining with a unique ID
     */
    @Test
    public void nodeUniqueIdNoDeletions() {
        final MembershipView mview = new MembershipView(K);
        int numExceptions = 0;
        final HostAndPort n1 = HostAndPort.fromParts("127.0.0.1", 1);
        final UUID id1 = UUID.randomUUID();
        try {
            mview.ringAdd(n1, id1);
        } catch (Exception e) {
            fail();
        }

        final HostAndPort n2 = HostAndPort.fromParts("127.0.0.1", 1);
        final UUID id2 = UUID.fromString(id1.toString());

        // Same host, same ID
        try {
            mview.ringAdd(n2, id2);
        }
        catch (final Exception e) {
            numExceptions++;
        }
        assertEquals(1, numExceptions);

        // Same host, different ID
        try {
            mview.ringAdd(n2, UUID.randomUUID());
        }
        catch (final Exception e) {
            numExceptions++;
        }
        assertEquals(2, numExceptions);

        // different host, same ID
        final HostAndPort n3 = HostAndPort.fromParts("127.0.0.1", 2);
        try {
            mview.ringAdd(n3, id2);
        }
        catch (final Exception e) {
            numExceptions++;
        }
        assertEquals(3, numExceptions);

        // different host, different ID
        try {
            mview.ringAdd(n3, UUID.randomUUID());
        }
        catch (final Exception e) {
            numExceptions++;
        }
        // Should not have triggered an exception
        assertEquals(3, numExceptions);

        // Only n1 and n3 should have been added
        assertEquals(2, mview.viewRing(0).size());
    }


    /**
     * Test for different combinations of a host joining with a unique ID
     */
    @Test
    public void nodeUniqueIdWithDeletions() {
        final MembershipView mview = new MembershipView(K);

        final HostAndPort n1 = HostAndPort.fromParts("127.0.0.1", 1);
        final UUID id1 = UUID.randomUUID();
        try {
            mview.ringAdd(n1, id1);
        } catch (final Exception e) {
            fail();
        }

        final HostAndPort n2 = HostAndPort.fromParts("127.0.0.1", 2);
        final UUID id2 = UUID.randomUUID();

        // Same host, same ID
        try {
            mview.ringAdd(n2, id2);
        }
        catch (final Exception e) {
            fail();
        }

        // Node is removed from the ring
        try {
            mview.ringDelete(n2);
        }
        catch (final Exception e) {
            fail();
        }
        assertEquals(1, mview.viewRing(0).size());

        int numExceptions = 0;
        // Node rejoins with id2
        try {
            mview.ringAdd(n2, id2);
        }
        catch (final Exception e) {
            numExceptions++;
        }
        assertEquals(1, numExceptions);

        // Re-attempt with new ID
        try {
            mview.ringAdd(n2, UUID.randomUUID());
        }
        catch (final Exception e) {
            fail();
        }

        assertEquals(2, mview.viewRing(0).size());
    }


    /**
     * Test for different combinations of a host joining with a unique ID
     */
    @Test
    public void nodeConfigurationChange() {
        final MembershipView mview = new MembershipView(K);
        final int numNodes = 1000;
        final Set<Long> set = new HashSet<>(numNodes);

        for (int i = 0; i < numNodes; i++) {
            final HostAndPort n = HostAndPort.fromParts("127.0.0.1", i);
            try {
                mview.ringAdd(n, UUID.nameUUIDFromBytes(n.toString().getBytes()));
                set.add(mview.getCurrentConfigurationId());
            } catch (final Exception e) {
                fail();
            }
        }
        assertEquals(numNodes, set.size()); // should be 1000 different configurations
    }


    /**
     * Test for different combinations of a host joining with a unique ID
     */
    @Test
    public void nodeConfigurationsAcrossMViews() {
        final MembershipView mview1 = new MembershipView(K);
        final MembershipView mview2 = new MembershipView(K);
        final int numNodes = 1000;
        final Set<Long> set1 = new HashSet<>(numNodes);
        final Set<Long> set2 = new HashSet<>(numNodes);

        for (int i = 0; i < numNodes; i++) {
            final HostAndPort n = HostAndPort.fromParts("127.0.0.1", i);
            try {
                mview1.ringAdd(n, UUID.nameUUIDFromBytes(n.toString().getBytes()));
                set1.add(mview1.getCurrentConfigurationId());

            } catch (final Exception e) {
                fail();
            }
        }

        for (int i = numNodes; i > 0; i--) {
            final HostAndPort n = HostAndPort.fromParts("127.0.0.1", i);
            try {
                mview2.ringAdd(n, UUID.nameUUIDFromBytes(n.toString().getBytes()));
                set2.add(mview2.getCurrentConfigurationId());
            } catch (final Exception e) {
                fail();
            }
        }

        assertEquals(numNodes, set1.size());
        assertEquals(numNodes, set2.size());

        final Iterator<Long> iter1 = set1.iterator();
        final Iterator<Long> iter2 = set1.iterator();
        for (int i = 0; i < numNodes; i++) {
            assertEquals(iter1.next(), iter2.next());
        }
    }
}