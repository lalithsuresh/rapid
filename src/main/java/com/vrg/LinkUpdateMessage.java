package com.vrg;

import com.google.common.net.HostAndPort;
import com.vrg.thrift.Status;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.framework.qual.DefaultQualifier;
import org.checkerframework.framework.qual.TypeUseLocation;

/**
 * LinkUpdateMessage is used by nodes to announce changes in link status
 */
@DefaultQualifier(value = NonNull.class, locations = TypeUseLocation.ALL)
final class LinkUpdateMessage {
    private final HostAndPort src;
    private final HostAndPort dst;
    private final Status status;

    LinkUpdateMessage(final HostAndPort src,
                      final HostAndPort dst,
                      final Status status) {
        this.src = src;
        this.dst = dst;
        this.status = status;
    }

    LinkUpdateMessage(final String src,
                      final String dst,
                      final Status status) {
        this.src = HostAndPort.fromString(src);
        this.dst = HostAndPort.fromString(dst);
        this.status = status;
    }

    HostAndPort getSrc() {
        return src;
    }

    HostAndPort getDst() {
        return dst;
    }

    Status getStatus() {
        return status;
    }
}