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
     * A series of updates.
     */
    @Test
    public void waterMarkTest() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final InetSocketAddress src = InetSocketAddress.createUnresolved("127.0.0.1", 1);
        final InetSocketAddress dst = InetSocketAddress.createUnresolved("127.0.0.2", 2);

        for (int i = 0; i < H - 1; i++) {
            wb.receiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP));
            assertEquals(0, wb.getNumDelivers());
        }

        wb.receiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP));
        assertEquals(1, wb.getNumDelivers());
    }

    @Test
    public void waterMarkTestBlocking() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final InetSocketAddress src = InetSocketAddress.createUnresolved("127.0.0.1", 1);
        final InetSocketAddress dst = InetSocketAddress.createUnresolved("127.0.0.2", 2);

        for (int i = 0; i < H - 1; i++) {
            wb.receiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP));
            assertEquals(0, wb.getNumDelivers());
        }

        for (int i = 0; i < H - 1; i++) {
            wb.receiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP));
            assertEquals(0, wb.getNumDelivers());
        }

        wb.receiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP));
        assertEquals(1, wb.getNumDelivers());
    }
}
