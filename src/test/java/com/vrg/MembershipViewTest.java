package com.vrg;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class MembershipViewTest {
    @Test
    public void oneRingAddition() {
        int K = 10;
        MembershipView mview = new MembershipView(K);
        InetSocketAddress addr = InetSocketAddress.createUnresolved("127.0.0.1", 123);
        Node node = new Node(addr);
        mview.ringAdd(node);

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
        int K = 10;
        MembershipView mview = new MembershipView(K);

        int numNodes = 10;

        for (int i = 0; i < numNodes; i++) {
            mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
        }
        for (int k = 0; k < K; k++) {
            List<Node> list = mview.viewRing(k);
            assertEquals(numNodes, list.size());
        }
    }

    @Test
    public void ringReAdditions() {
        int K = 10;
        MembershipView mview = new MembershipView(K);

        int numNodes = 10;
        int startPort = 0;

        for (int i = 0; i < numNodes; i++) {
            mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", startPort + i)));
        }

        for (int k = 0; k < K; k++) {
            List<Node> list = mview.viewRing(k);
            assertEquals(numNodes, list.size());
        }

        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", startPort + i)));
            } catch (RuntimeException e) {
                numThrows++;
            }
        }

        assertEquals(numNodes, numThrows);
    }

    @Test
    public void ringDeletionsOnly() {
        int K = 10;
        MembershipView mview = new MembershipView(K);

        int numNodes = 10;
        int numThrows = 0;
        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
            } catch (RuntimeException e) {
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
            mview.ringAdd(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
        }

        for (int i = 0; i < numNodes; i++) {
            try {
                mview.ringDelete(new Node(InetSocketAddress.createUnresolved("127.0.0.1", i)));
            } catch (RuntimeException e) {
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
        int K = 10;
        MembershipView mview = new MembershipView(K);
        Node n1 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 1));
        mview.ringAdd(n1);
        assertEquals(0, mview.monitoreesOf(n1).size());
        assertEquals(0, mview.monitorsOf(n1).size());

        Node n2 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 2));
        assertEquals(0, mview.monitoreesOf(n2).size());
        assertEquals(0, mview.monitorsOf(n2).size());
    }

    @Test
    public void monitoringRelationshipEmpty() {
        int K = 10;
        MembershipView mview = new MembershipView(K);
        Node n = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 1));
        assertEquals(0, mview.monitoreesOf(n).size());
        assertEquals(0, mview.monitorsOf(n).size());
    }

    @Test
    public void monitoringRelationshipTwoNodes() {
        int K = 10;
        MembershipView mview = new MembershipView(K);
        Node n1 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 1));
        Node n2 = new Node(InetSocketAddress.createUnresolved("127.0.0.1", 2));
        mview.ringAdd(n1);
        mview.ringAdd(n2);
        assertEquals(1, mview.monitoreesOf(n1).size());
        assertEquals(1, mview.monitorsOf(n1).size());
    }

    @Test
    public void monitoringRelationshipThreeNodesWithDelete() {
        int K = 10;
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
    }

    @Test
    public void monitoringRelationshipMultipleNodes() {
        int K = 10;
        MembershipView mview = new MembershipView(K);

        int numNodes = 1000;
        ArrayList<Node> list = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Node n = new Node(InetSocketAddress.createUnresolved("127.0.0.1", i));
            list.add(n);
            mview.ringAdd(n);
        }

        for (int i = 0; i < numNodes; i++) {
            int numMonitorees = mview.monitoreesOf(list.get(i)).size();
            int numMonitors = mview.monitoreesOf(list.get(i)).size();
            assertTrue("NumMonitorees: " + numMonitorees, K - 3 <= numMonitorees);
            assertTrue("NumMonitorees: " + numMonitorees, K >= numMonitorees);
            assertTrue("NumMonitors: " + numMonitors, K - 3 <= numMonitors);
            assertTrue("NumMonitors: " + numMonitors,K >= numMonitors);
        }
    }
}