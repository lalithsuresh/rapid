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
import com.vrg.rapid.pb.Remoting.Status;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

final class TestUtils {

    private TestUtils() {
    }

    static LinkUpdateMessage[] getMessagesArray(final int K) {
        final String ip1 = "192.168.1.1";
        final int startingPort = 1;
        final HostAndPort src1 = HostAndPort.fromParts(ip1, startingPort);
        final LinkUpdateMessage[] messages = new LinkUpdateMessage[2*K];
        int arrIndex = 0;
        for (int i = 0; i < K; i++) {
            messages[arrIndex] = new LinkUpdateMessage(src1,
                    HostAndPort.fromParts(ip1, startingPort + i + 1),
                    Status.UP);
            arrIndex++;
        }

        final String ip2 = "10.1.1.1";
        final HostAndPort src2 = HostAndPort.fromParts(ip2, startingPort + K + 1);
        for (int i = 0; i < K; i++) {
            messages[arrIndex] = new LinkUpdateMessage(src2,
                    HostAndPort.fromParts(ip2, startingPort + K + i + 2),
                    Status.UP);
            arrIndex++;
        }
        return messages;
    }

    // Fisher–Yates shuffle
    static void shuffleArray(final LinkUpdateMessage[] ar)
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
