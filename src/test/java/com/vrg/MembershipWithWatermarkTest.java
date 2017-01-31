package com.vrg;

import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for a MembershipView backed by a WatermarkBuffer.
 */
public class MembershipWithWatermarkTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;

    /**
     * A ring initialized with self
     */
    @Test
    public void oneRingAddition() {
        final InetSocketAddress addr = InetSocketAddress.createUnresolved("127.0.0.1", 123);
        final MembershipView mview = new MembershipView(K, new Node(addr));
        for (int k = 0; k < K; k++) {
            final List<Node> list = mview.viewRing(k);
            assertEquals(1, list.size());
            for (final Node n : list) {
                assertEquals(n.address, addr);
            }
        }
    }

    /**
     * A series of updates in increasing order of incarnations.
     * At any incarnation X maintained by a node of a peer, a new incarnation
     * is always <= X
     */
    @Ignore("Affected by configuration work") @Test
    public void multipleUpdatesOrderedIncarnations() {
        final int numPermutations = 100000;
        final MembershipView mview = new MembershipView(K);
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, mview::deliver);

        int numFlushes = 0;
        for (int i = 0; i < numPermutations; i++) {
            final LinkUpdateMessage[] messages = TestUtils.getMessagesArray(K);
            TestUtils.shuffleArray(messages);
            String eventStream = "";
            for (final LinkUpdateMessage msg: messages) {
                final int result = wb.ReceiveLinkUpdateMessage(msg).size();
                final String log = msg.getSrc() + " " + result + " \n";
                eventStream += log;
            }

            assertTrue(eventStream + " " + numFlushes,
                    numFlushes + 1 == wb.getNumDelivers()
                            || numFlushes + 2 == wb.getNumDelivers());
            numFlushes = wb.getNumDelivers();
        }

        assertEquals(mview.viewRing(0).size(), 2);
    }

    /**
     * A series of updates with a scrambled order of incarnations.
     */
    @Ignore("Affected by configuration work") @Test
    public void multipleUpdatesUnorderedIncarnations() {
        final int numIncarnations = 10;
        final int numPermutations = 100000;

        final MembershipView mview = new MembershipView(K);
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, mview::deliver);

        int numFlushes = 0;

        final ArrayList<LinkUpdateMessage> list =  new ArrayList<>();

        for (int i = 0; i < numIncarnations; i++) {
            list.addAll(Arrays.asList(TestUtils.getMessagesArray(K)));
        }

        final LinkUpdateMessage[] messages = list.toArray(new LinkUpdateMessage[list.size()]);
        for (int i = 0; i < numPermutations; i++) {
            TestUtils.shuffleArray(messages);
            String eventStream = "";
            for (final LinkUpdateMessage msg: messages) {
                final int result = wb.ReceiveLinkUpdateMessage(msg).size();
                final String log = msg.getSrc() + " " + result + " \n";
                eventStream += log;
            }

            assertTrue(eventStream + " " + numFlushes,
                    numFlushes + 1 == wb.getNumDelivers()
                            || numFlushes + 2 == wb.getNumDelivers());
            numFlushes = wb.getNumDelivers();
        }

        assertEquals(mview.viewRing(0).size(), 2);
    }
}
