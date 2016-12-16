package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.net.InetSocketAddress;

/**
mo * Created by lsuresh on 12/14/16.
 */
public class MembershipService {
    @NonNull private final MembershipView membershipView;
    @NonNull private final InetSocketAddress myAddr;

    public MembershipService(@NonNull final InetSocketAddress myAddr, final int K) {
        this.membershipView = new MembershipView(K);
        this.myAddr = myAddr;
        this.membershipView.ringAdd(new Node(this.myAddr));
    }

    public void ReceiveLinkUpdateMessage(@NonNull final LinkUpdateMessage msg) {
        // watermarker add
    }
}