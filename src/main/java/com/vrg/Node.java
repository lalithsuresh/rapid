package com.vrg;

import java.net.InetSocketAddress;

/**
 * Created by lsuresh on 12/13/16.
 */
public class Node {
    public final InetSocketAddress address;

    public Node(InetSocketAddress address) {
        this.address = address;
    }
}
