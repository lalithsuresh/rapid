package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.LinkStatus;

import java.util.Map;

/**
 * Represents a single node status change event. It is the format used to inform applications about
 * cluster view change events.
 */
public class NodeStatusChange {
    private final HostAndPort hostAndPort;
    private final LinkStatus status;
    private final Map<String, String> metadata;

    NodeStatusChange(final HostAndPort hostAndPort,
                     final LinkStatus status,
                     final Map<String, String> metadata) {
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

    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return String.valueOf(hostAndPort) + ":" + status + ":" + metadata;
    }
}