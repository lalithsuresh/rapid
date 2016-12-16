package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.net.InetSocketAddress;

/**
 * Created by lsuresh on 12/14/16.
 */
public class LinkUpdateMessage {
    @NonNull private final InetSocketAddress src;
    @NonNull private final InetSocketAddress dst;
    @NonNull private final Status status;
    @NonNull private final int incarnation;

    public LinkUpdateMessage(@NonNull final InetSocketAddress src,
                             @NonNull final InetSocketAddress dst,
                             @NonNull final Status status,
                             @NonNull final int incarnation) {
        this.src = src;
        this.dst = dst;
        this.status = status;
        this.incarnation = incarnation;
    }

    public enum Status {
        UP, DOWN
    }

    @NonNull public InetSocketAddress getSrc() {
        return src;
    }

    @NonNull public InetSocketAddress getDst() {
        return dst;
    }

    @NonNull public Status getStatus() {
        return status;
    }

    @NonNull public int getIncarnation() {
        return incarnation;
    }
}
