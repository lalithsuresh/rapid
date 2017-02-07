package com.vrg;

import com.google.common.net.HostAndPort;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void oneRingAddition() {
        final HostAndPort addr = HostAndPort.fromParts("127.0.0.1", 123);
        final MembershipView mview = new MembershipView(K, new Node(addr));
        for (int k = 0; k < K; k++) {
            final List<Node> list = mview.viewRing(k);
            assertEquals(1, list.size());
            for (final Node n : list) {
                assertEquals(n.address, addr);
            }
        }
    }
}
