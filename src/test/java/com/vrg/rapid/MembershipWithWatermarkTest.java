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
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

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
    public void oneRingAddition() throws MembershipView.NodeAlreadyInRingException {
        final HostAndPort addr = HostAndPort.fromParts("127.0.0.1", 123);
        final MembershipView mview = new MembershipView(K);
        mview.ringAdd(addr, UUID.randomUUID());
        for (int k = 0; k < K; k++) {
            final List<HostAndPort> list = mview.viewRing(k);
            assertEquals(1, list.size());
            for (final HostAndPort n : list) {
                assertEquals(n, addr);
            }
        }
    }
}
