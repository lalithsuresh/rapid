package com.vrg;

import org.junit.Ignore;
import org.junit.Test;
import java.net.InetSocketAddress;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Tests without changing incarnations for a watermark-buffer
 */
public class WatermarkBufferTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;

    /**
     * A series of updates in increasing order of incarnations.
     * At any incarnation X maintained by a node of a peer, a new incarnation
     * is always <= X
     */
    @Test
    public void waterMarkTest() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, this::emptyConsumer);
        final InetSocketAddress src = InetSocketAddress.createUnresolved("127.0.0.1", 1);
        final InetSocketAddress dst = InetSocketAddress.createUnresolved("127.0.0.2", 2);

        for (int i = 0; i < 7; i++) {
            wb.ReceiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP));
            assertEquals(0, wb.getNumDelivers());
        }

        wb.ReceiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP));
        assertEquals(1, wb.getNumDelivers());
    }

    /**
     * Permutations of LinkUpdateMessage arrivals pertaining to two nodes are pushed.
     * Ensure that the number of deliver events are sane. Ideally, we'd look at the
     * resulting distribution (it should mostly be single view updates and not an
     * update each for each node).
     */
    @Ignore("Affected by configuration work") @Test
    public void watermarkTwoAnnouncementsPermutation() {
        final int numPermutations = 100000;
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L, this::emptyConsumer);

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
    }

    private void emptyConsumer(final LinkUpdateMessage msg) {
    }
}
