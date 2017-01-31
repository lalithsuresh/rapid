package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

final class TestUtils {

    private TestUtils() {
    }

    @NonNull
    static LinkUpdateMessage[] getMessagesArray(final int K) {
        final String ip1 = "192.168.1.1";
        final int startingPort = 1;
        final InetSocketAddress src1 = InetSocketAddress.createUnresolved(ip1, startingPort);
        final LinkUpdateMessage[] messages = new LinkUpdateMessage[2*K];
        int arrIndex = 0;
        for (int i = 0; i < K; i++) {
            messages[arrIndex] = new LinkUpdateMessage(src1,
                    InetSocketAddress.createUnresolved(ip1, startingPort + i + 1),
                    LinkUpdateMessage.Status.UP);
            arrIndex++;
        }

        final String ip2 = "10.1.1.1";
        final InetSocketAddress src2 = InetSocketAddress.createUnresolved(ip2, startingPort + K + 1);
        for (int i = 0; i < K; i++) {
            messages[arrIndex] = new LinkUpdateMessage(src2,
                    InetSocketAddress.createUnresolved(ip2, startingPort + K + i + 2),
                    LinkUpdateMessage.Status.UP);
            arrIndex++;
        }
        return messages;
    }

    // Fisher–Yates shuffle
    static void shuffleArray(@NonNull LinkUpdateMessage[] ar)
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
}
