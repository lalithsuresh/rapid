package com.vrg.rapid;

/**
 * Event types to subscribe from the cluster.
 */
public enum ClusterEvents {        // Triggered when...
    VIEW_CHANGE_PROPOSAL,          // <- When a node announces a proposal using the watermark detection.
    VIEW_CHANGE,                   // <- When a fast-paxos quorum of identical proposals were received.
    VIEW_CHANGE_ONE_STEP_FAILED,   // <- When a fast-paxos quorum of identical proposals is unavailable
}
