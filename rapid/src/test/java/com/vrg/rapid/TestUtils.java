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

import com.vrg.rapid.pb.LinkUpdateMessage;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

final class TestUtils {

    private TestUtils() {
    }

    // Fisher–Yates shuffle
    static void shuffleArray(final LinkUpdateMessage[] ar) {
        final Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--) {
            final int index = rnd.nextInt(i + 1);
            final LinkUpdateMessage a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }
}
