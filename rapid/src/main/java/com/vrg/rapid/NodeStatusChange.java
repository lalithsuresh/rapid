package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.LinkStatus;

import java.util.List;

/**
 * Represents a single node status change event. It is the format used to inform applications about
 * cluster view change events.
 */
public class NodeStatusChange {
    private final HostAndPort hostAndPort;
    private final LinkStatus status;
    private final List<String> roles;

    NodeStatusChange(final HostAndPort hostAndPort,
                            final LinkStatus status,
                            final List<String> roles) {
        this.hostAndPort = hostAndPort;
        this.status = status;
        this.roles = roles;
    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    public LinkStatus getStatus() {
        return status;
    }

    public List<String> getRoles() {
        return roles;
    }

    @Override
    public String toString() {
        return String.valueOf(hostAndPort) + ":" + status + ":" + roles;
    }
}