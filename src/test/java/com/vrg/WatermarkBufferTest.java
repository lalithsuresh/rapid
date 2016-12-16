package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by lsuresh on 12/15/16.
 */
public class WatermarkBufferTest {
    private static final int K = 10;

    @Test
    public void waterMarkTest() {
        final WatermarkBuffer wb = new WatermarkBuffer(K);
        final InetSocketAddress src = InetSocketAddress.createUnresolved("127.0.0.1", 1);
        final InetSocketAddress dst = InetSocketAddress.createUnresolved("127.0.0.2", 2);
        final int incarnation = 1;

        for (int i = 0; i < 7; i++) {
            wb.ReceiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP, incarnation));
            assertEquals(0, wb.flushCounter.get());
        }
        wb.ReceiveLinkUpdateMessage(new LinkUpdateMessage(src, dst, LinkUpdateMessage.Status.UP, incarnation));
        assertEquals(1, wb.flushCounter.get());
    }

    @Test
    public void watermarkTwoAnnouncementsPermuation() {
        final int numPermutations = 10000;
        int incarnations = 0;
        final WatermarkBuffer wb = new WatermarkBuffer(K);

        int numFlushes = 0;
        for (int i = 0; i < numPermutations; i++) {
            LinkUpdateMessage[] messages = getMessagesArray(incarnations++);
            shuffleArray(messages);
            String eventStream = "";
            for (LinkUpdateMessage msg: messages) {
                final int result = wb.ReceiveLinkUpdateMessage(msg);
                String log = msg.getSrc() + " " + result + " \n";
                eventStream += log;
            }

            assertTrue(eventStream + " " + numFlushes,
                    numFlushes + 1 == wb.flushCounter.get()
                    || numFlushes + 2 == wb.flushCounter.get());
            numFlushes = wb.flushCounter.get();
        }
    }

    // Fisher–Yates shuffle
    private static void shuffleArray(@NonNull LinkUpdateMessage[] ar)
    {
        final Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--)
        {
            final int index = rnd.nextInt(i + 1);
            final LinkUpdateMessage a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    @NonNull private LinkUpdateMessage[] getMessagesArray(int incarnation) {
        final String ip1 = "192.168.1.1";
        final int startingPort = 1;
        InetSocketAddress src1 = InetSocketAddress.createUnresolved(ip1, startingPort);
        LinkUpdateMessage[] messages = new LinkUpdateMessage[2*K];
        int arrIndex = 0;
        for (int i = 0; i < K; i++) {
            messages[arrIndex] = new LinkUpdateMessage(src1,
                    InetSocketAddress.createUnresolved(ip1, startingPort + i + 1),
                    LinkUpdateMessage.Status.UP,
                    incarnation);
            arrIndex++;
        }

        final String ip2 = "10.1.1.1";
        InetSocketAddress src2 = InetSocketAddress.createUnresolved(ip2, startingPort + K + 1);
        for (int i = 0; i < K; i++) {
            messages[arrIndex] = new LinkUpdateMessage(src2,
                    InetSocketAddress.createUnresolved(ip2, startingPort + K + i + 2),
                    LinkUpdateMessage.Status.UP,
                    incarnation);
            arrIndex++;
        }
        return messages;
    }
}
