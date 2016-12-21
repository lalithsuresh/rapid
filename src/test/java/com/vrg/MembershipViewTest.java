package com.vrg;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MembershipViewTest {

    @Test
    public void oneRingAddition() {
        final int K = 10;
        MembershipView mview = new MembershipView(K);
        InetSocketAddress addr = InetSocketAddress.createUnresolved("127.0.0.1", 123);
        Node node = new Node(addr);
        try {
            mview.ringAdd(node);
        } catch (MembershipView.NodeAlreadyInRingException e) {
            fail();
        }

        for (int k = 0; k < K; k++) {
            List<Node> list = mview.viewRing(k);
            assertEquals(1, list.size());
            for (Node n : list) {
                assertEquals(n.address, addr);
            }
        }
    }

    @Test
    public void multipleRingAdditions() {
        final int K = 10;
        MembershipView mview = new MembershipView(K);

        final int numNodes = 10;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
            } catch (MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }
        for (int k = 0; k < K; k++) {
            List<Node> list = mview.viewRing(k);
            assertEquals(numNodes, list.size());
        }
    }

    @Test
    public void ringReAdditions() {
        final int K = 10;
        MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        final int startPort = 0;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", startPort + i)));
            } catch (MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int k = 0; k < K; k++) {
            List<Node> list = mview.viewRing(k);
            assertEquals(numNodes, list.size());
        }

        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", startPort + i)));
            } catch (MembershipView.NodeAlreadyInRingException e) {
                numThrows++;
            }
        }

        assertEquals(numNodes, numThrows);
    }

    @Test
    public void ringDeletionsOnly() {
        final int K = 10;
        MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
            } catch (MembershipView.NodeNotInRingException e) {
                numThrows++;
            }
        }

        assertEquals(numNodes, numThrows);
    }

    @Test
    public void ringAdditionsAndDeletions() {
        final int K = 10;
        MembershipView mview = new MembershipView(K);

        final int numNodes = 10;
        int numThrows = 0;

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
            } catch (MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
            } catch (MembershipView.NodeNotInRingException e) {
                numThrows++;
            }
        }

        assertEquals(0, numThrows);

        for (int k = 0; k < K; k++) {
            List<Node> list = mview.viewRing(k);
            assertEquals(0, list.size());
        }
    }

    @Test
    public void monitoringRelationshipEdge() {
        try {
            final int K = 10;
            MembershipView mview = new MembershipView(K);
            Node n1 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 1));
            mview.ringAdd(n1);
            assertEquals(0, mview.monitoreesOf(n1).size());
            assertEquals(0, mview.monitorsOf(n1).size());

            Node n2 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 2));
            assertEquals(0, mview.monitoreesOf(n2).size());
            assertEquals(0, mview.monitorsOf(n2).size());
        } catch (MembershipView.NodeAlreadyInRingException e) {
            fail();
        } catch (MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    @Test
    public void monitoringRelationshipEmpty() {
        try {
            final int K = 10;
            MembershipView mview = new MembershipView(K);
            Node n = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 1));
            assertEquals(0, mview.monitoreesOf(n).size());
            assertEquals(0, mview.monitorsOf(n).size());
        } catch (MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    @Test
    public void monitoringRelationshipTwoNodes() {
        try {
            final int K = 10;
            MembershipView mview = new MembershipView(K);
            Node n1 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 1));
            Node n2 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 2));
            mview.ringAdd(n1);
            mview.ringAdd(n2);
            assertEquals(1, mview.monitoreesOf(n1).size());
            assertEquals(1, mview.monitorsOf(n1).size());
        } catch (MembershipView.NodeAlreadyInRingException e) {
            fail();
        } catch (MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    @Test
    public void monitoringRelationshipThreeNodesWithDelete() {
        try {
            final int K = 10;
            MembershipView mview = new MembershipView(K);
            Node n1 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 1));
            Node n2 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 2));
            Node n3 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 3));
            mview.ringAdd(n1);
            mview.ringAdd(n2);
            mview.ringAdd(n3);
            assertEquals(2, mview.monitoreesOf(n1).size());
            assertEquals(2, mview.monitorsOf(n1).size());
            mview.ringDelete(n2);
            assertEquals(1, mview.monitoreesOf(n1).size());
            assertEquals(1, mview.monitorsOf(n1).size());
        } catch (MembershipView.NodeAlreadyInRingException e) {
            fail();
        } catch (MembershipView.NodeNotInRingException e) {
            fail();
        }
    }

    @Test
    public void monitoringRelationshipMultipleNodes() {
        final int K = 10;
        MembershipView mview = new MembershipView(K);

        final int numNodes = 10000;
        ArrayList<Node> list = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Node n = new Node(InetSocketAddress.createUnresolved("127.0.0.1", i));
            list.add(n);
            try {
                mview.ringAdd(n);
            } catch (MembershipView.NodeAlreadyInRingException e) {
                fail();
            }
        }

        for (int i = 0; i < numNodes; i++) {
            try {
                int numMonitorees = mview.monitoreesOf(list.get(i)).size();
                int numMonitors = mview.monitoreesOf(list.get(i)).size();
                assertTrue("NumMonitorees: " + numMonitorees, K - 3 <= numMonitorees);
                assertTrue("NumMonitorees: " + numMonitorees, K >= numMonitorees);
                assertTrue("NumMonitors: " + numMonitors, K - 3 <= numMonitors);
                assertTrue("NumMonitors: " + numMonitors, K >= numMonitors);
            } catch (MembershipView.NodeNotInRingException e) {
                fail();
            }
        }
    }
}