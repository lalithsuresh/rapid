package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.framework.qual.DefaultQualifier;
import org.checkerframework.framework.qual.TypeUseLocation;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by MembershipService
 */
@DefaultQualifier(value = NonNull.class, locations = TypeUseLocation.ALL)
public class MembershipService {
    private final MembershipView membershipView;
    private final WatermarkBuffer watermarkBuffer;
    private final InetSocketAddress myAddr;

    public MembershipService(final InetSocketAddress myAddr,
                             final int K, final int H, final int L) {
        this.myAddr = myAddr;
        this.membershipView = new MembershipView(K, new Node(this.myAddr));
        this.watermarkBuffer = new WatermarkBuffer(K, H, L);
    }

    /**
     * This method receives link update events and delivers them to
     * the watermark buffer to check if it will return a valid
     * proposal.
     *
     * Link update messages that do not affect an ongoing proposal
     * needs to be dropped.
     *
     *
     */
    public void receiveLinkUpdateMessage(final LinkUpdateMessage msg) {
        final List<Node> proposal = proposedViewChange(msg);
        if (proposal.size() != 0) {
            // Initiate proposal
            throw new UnsupportedOperationException();
            // throw in consensus engine.
        }

        // continue gossipping
    }

    List<Node> proposedViewChange(final LinkUpdateMessage msg) {
        return watermarkBuffer.receiveLinkUpdateMessage(msg);
    }
}