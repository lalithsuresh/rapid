package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.framework.qual.DefaultQualifier;
import org.checkerframework.framework.qual.TypeUseLocation;

import java.net.InetSocketAddress;

/**
 * Created by lsuresh on 12/14/16.
 */
@DefaultQualifier(value = NonNull.class, locations = TypeUseLocation.ALL)
public class MembershipService {
    @NonNull private final MembershipView membershipView;
    @NonNull private final WatermarkBuffer watermarkBuffer;
    @NonNull private final InetSocketAddress myAddr;

    public MembershipService(@NonNull final InetSocketAddress myAddr,
                             final int K, final int H, final int L) {
        this.membershipView = new MembershipView(K);
        this.myAddr = myAddr;
        this.watermarkBuffer = new WatermarkBuffer(K, H, L, this.membershipView::deliver);
        this.membershipView.initializeWithSelf(new Node(this.myAddr));
    }

    public void ReceiveLinkUpdateMessage(@NonNull final LinkUpdateMessage msg) {
        watermarkBuffer.ReceiveLinkUpdateMessage(msg);
    }
}