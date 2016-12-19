package com.vrg;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.framework.qual.DefaultQualifier;
import org.checkerframework.framework.qual.TypeUseLocation;

import java.net.InetSocketAddress;

/**
 * Created by lsuresh on 12/14/16.
 */
@DefaultQualifier(value = NonNull.class, locations = TypeUseLocation.ALL)
public class LinkUpdateMessage {
    private final InetSocketAddress src;
    private final InetSocketAddress dst;
    private final Status status;
    private final int incarnation;

    public LinkUpdateMessage(final InetSocketAddress src,
                             final InetSocketAddress dst,
                             final Status status,
                             final int incarnation) {
        this.src = src;
        this.dst = dst;
        this.status = status;
        this.incarnation = incarnation;
    }

    public enum Status {
        UP, DOWN
    }

    public InetSocketAddress getSrc() {
        return src;
    }

    public InetSocketAddress getDst() {
        return dst;
    }

    public Status getStatus() {
        return status;
    }

    public int getIncarnation() {
        return incarnation;
    }
}
