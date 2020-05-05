package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;

import java.util.concurrent.atomic.AtomicBoolean;

public class TestMembershipView extends MembershipView {

    private AtomicBoolean shouldWait = new AtomicBoolean(false);

    public TestMembershipView(final int K) {
        super(K);
    }

    public void toggleWait() {
        final boolean current = shouldWait.get();
        shouldWait.set(!current);
    }

    @Override
    boolean isHostPresent(final Endpoint address) {
        try {
            if (shouldWait.get()) {
                Thread.sleep(200);
            }
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
        return super.isHostPresent(address);
    }
}
