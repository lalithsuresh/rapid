package com.vrg;

import com.google.common.net.HostAndPort;
import com.vrg.thrift.LinkUpdateMessageT;
import com.vrg.thrift.Status;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.framework.qual.DefaultQualifier;
import org.checkerframework.framework.qual.TypeUseLocation;

/**
 * LinkUpdateMessage is used by nodes to announce changes in link status
 */
@DefaultQualifier(value = NonNull.class, locations = TypeUseLocation.ALL)
public class LinkUpdateMessage {
    private final HostAndPort src;
    private final HostAndPort dst;
    private final Status status;

    public LinkUpdateMessage(final HostAndPort src,
                             final HostAndPort dst,
                             final Status status) {
        this.src = src;
        this.dst = dst;
        this.status = status;
    }

    // Convert from a thrift message
    public LinkUpdateMessage(final LinkUpdateMessageT msgt) {
        new LinkUpdateMessageT();
        this.src = HostAndPort.fromString(msgt.getSrc());
        this.dst = HostAndPort.fromString(msgt.getDst());
        this.status = msgt.getOp();
    }

    public HostAndPort getSrc() {
        return src;
    }

    public HostAndPort getDst() {
        return dst;
    }

    public Status getStatus() {
        return status;
    }
}