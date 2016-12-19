package com.vrg;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by lsuresh on 12/16/16.
 */
public class MembershipWithWatermarkTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;

    @Test
    public void oneRingAddition() {
        final InetSocketAddress addr = InetSocketAddress.createUnresolved("127.0.0.1", 123);
        final MembershipView mview = new MembershipView(K);
        mview.initializeWithSelf(new Node(addr));
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, mview::deliver);
        for (int k = 0; k < K; k++) {
            final List<Node> list = mview.viewRing(k);
            assertEquals(1, list.size());
            for (Node n : list) {
                assertEquals(n.address, addr);
            }
        }
    }

    @Test
    public void multipleUpdatesOrderedIncarnations() {
        final int numPermutations = 100000;
        int incarnations = 0;
        final MembershipView mview = new MembershipView(K);
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, mview::deliver);

        int numFlushes = 0;
        for (int i = 0; i < numPermutations; i++) {
            final LinkUpdateMessage[] messages = TestUtils.getMessagesArray(incarnations++, K);
            TestUtils.shuffleArray(messages);
            String eventStream = "";
            for (LinkUpdateMessage msg: messages) {
                final int result = wb.ReceiveLinkUpdateMessage(msg);
                String log = msg.getSrc() + " " + result + " \n";
                eventStream += log;
            }

            assertTrue(eventStream + " " + numFlushes,
                    numFlushes + 1 == wb.getNumDelivers()
                            || numFlushes + 2 == wb.getNumDelivers());
            numFlushes = wb.getNumDelivers();
        }

        assertEquals(mview.viewRing(0).size(), 2);
    }


    @Test
    public void multipleUpdatesUnOrderedIncarnations() {
        final int numIncarnations = 10;
        final int numPermutations = 100000;

        final MembershipView mview = new MembershipView(K);
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, mview::deliver);

        int numFlushes = 0;

        ArrayList<LinkUpdateMessage> list =  new ArrayList<>();

        for (int i = 0; i < numIncarnations; i++) {
            list.addAll(Arrays.asList(TestUtils.getMessagesArray(i, K)));
        }

        LinkUpdateMessage[] messages = list.toArray(new LinkUpdateMessage[list.size()]);
        for (int i = 0; i < numPermutations; i++) {
            TestUtils.shuffleArray(messages);
            String eventStream = "";
            for (LinkUpdateMessage msg: messages) {
                final int result = wb.ReceiveLinkUpdateMessage(msg);
                String log = msg.getSrc() + " " + result + " \n";
                eventStream += log;
            }

            assertTrue(eventStream + " " + numFlushes,
                    numFlushes + 1 == wb.getNumDelivers()
                            || numFlushes + 2 == wb.getNumDelivers());
            numFlushes = wb.getNumDelivers();
        }

        assertEquals(mview.viewRing(0).size(), 2);
    }


    @Test
    public void multipleUpdates() {

    }

    private void emptyConsumer(LinkUpdateMessage msg) {
    }
}
