package com.vrg;

import org.junit.Test;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class WatermarkBufferTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;

    @Test
    public void waterMarkTest() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, this::emptyConsumer);
        final InetSocketAddress src = InetSocketAddress.createUnresolved("127.0.0.1", 1);
        final InetSocketAddress dst = InetSocketAddress.createUnresolved("127.0.0.2", 2);
        final int incarnation = 1;

        for (int i = 0; i < 7; i++) {
            wb.ReceiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP, incarnation));
            assertEquals(0, wb.getNumDelivers());
        }

        wb.ReceiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP, incarnation));
        assertEquals(1, wb.getNumDelivers());
    }

    @Test
    public void watermarkTwoAnnouncementsPermutation() {
        final int numPermutations = 100000;
        int incarnations = 0;
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, this::emptyConsumer);

        int numFlushes = 0;
        for (int i = 0; i < numPermutations; i++) {
            LinkUpdateMessage[] messages = TestUtils.getMessagesArray(incarnations++, K);
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
    }

    private void emptyConsumer(LinkUpdateMessage msg) {
    }
}
