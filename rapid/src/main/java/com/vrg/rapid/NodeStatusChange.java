package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.LinkStatus;
import com.vrg.rapid.pb.Metadata;

/**
 * Represents a single node status change event. It is the format used to inform applications about
 * cluster view change events.
 */
public class NodeStatusChange {
    private final HostAndPort hostAndPort;
    private final LinkStatus status;
    private final Metadata metadata;

    NodeStatusChange(final HostAndPort hostAndPort,
                     final LinkStatus status,
                     final Metadata metadata) {
        this.hostAndPort = hostAndPort;
        this.status = status;
        this.metadata = metadata;
    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    public LinkStatus getStatus() {
        return status;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return String.valueOf(hostAndPort) + ":" + status + ":" + metadata;
    }
}