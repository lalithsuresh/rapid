package com.vrg;

import com.google.common.net.HostAndPort;

import java.net.InetSocketAddress;

/**
 * Represents a node. For now, we just wrap it around a socket-address.
 */
class Node {
    final HostAndPort address;

    Node(final HostAndPort address) {
        this.address = address;
    }
}
