package com.vrg;

import java.net.InetSocketAddress;

/**
 * Represents a node. For now, we just wrap it around a socket-address.
 */
class Node {
    final InetSocketAddress address;

    Node(final InetSocketAddress address) {
        this.address = address;
    }
}
