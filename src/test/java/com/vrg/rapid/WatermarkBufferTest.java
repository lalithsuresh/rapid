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
import com.vrg.rapid.pb.Remoting.Status;
import java.util.List;

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
        final HostAndPort dst = HostAndPort.fromParts("127.0.0.2", 2);
        List<Node> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst, Status.UP));
        assertEquals(1, ret.size());
        assertEquals(1, wb.getNumDelivers());
    }

    @Test
    public void waterMarkTestBlockingOneBlocker() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final HostAndPort dst1 = HostAndPort.fromParts("127.0.0.2", 2);
        final HostAndPort dst2 = HostAndPort.fromParts("127.0.0.3", 2);
        List<Node> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst1, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst2, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst1, Status.UP));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumDelivers());

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst2, Status.UP));
        assertEquals(2, ret.size());
        assertEquals(1, wb.getNumDelivers());
    }


    @Test
    public void waterMarkTestBlockingThreeBlockers() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final HostAndPort dst1 = HostAndPort.fromParts("127.0.0.2", 2);
        final HostAndPort dst2 = HostAndPort.fromParts("127.0.0.3", 2);
        final HostAndPort dst3 = HostAndPort.fromParts("127.0.0.4", 2);
        List<Node> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst1, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst2, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst3, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst1, Status.UP));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumDelivers());

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst3, Status.UP));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumDelivers());

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst2, Status.UP));
        assertEquals(3, ret.size());
        assertEquals(1, wb.getNumDelivers());
    }

    @Test
    public void waterMarkTestBlockingMultipleBlockersPastH() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final HostAndPort dst1 = HostAndPort.fromParts("127.0.0.2", 2);
        final HostAndPort dst2 = HostAndPort.fromParts("127.0.0.3", 2);
        final HostAndPort dst3 = HostAndPort.fromParts("127.0.0.4", 2);
        List<Node> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst1, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst2, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst3, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        // Unlike the previous test, add more reports for
        // dst1 and dst2 past the H boundary.
        wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst1, Status.UP));
        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H + 1), dst1, Status.UP));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumDelivers());

        wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst3, Status.UP));
        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H + 1), dst3, Status.UP));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumDelivers());


        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst2, Status.UP));
        assertEquals(3, ret.size());
        assertEquals(1, wb.getNumDelivers());
    }

    @Test
    public void waterMarkTestBelowL() {
        final WatermarkBuffer wb = new WatermarkBuffer(K, H, L);
        final HostAndPort dst1 = HostAndPort.fromParts("127.0.0.2", 2);
        final HostAndPort dst2 = HostAndPort.fromParts("127.0.0.3", 2);
        final HostAndPort dst3 = HostAndPort.fromParts("127.0.0.4", 2);
        List<Node> ret;

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst1, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        // Unlike the previous test, dst2 has < L updates
        for (int i = 0; i < L - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst2, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        for (int i = 0; i < H - 1; i++) {
            ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                    HostAndPort.fromParts("127.0.0.1", i + 1), dst3, Status.UP));
            assertEquals(0, ret.size());
            assertEquals(0, wb.getNumDelivers());
        }

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst1, Status.UP));
        assertEquals(0, ret.size());
        assertEquals(0, wb.getNumDelivers());

        ret = wb.receiveLinkUpdateMessage(new LinkUpdateMessage(
                HostAndPort.fromParts("127.0.0.1", H), dst3, Status.UP));
        assertEquals(2, ret.size());
        assertEquals(1, wb.getNumDelivers());
    }
}
