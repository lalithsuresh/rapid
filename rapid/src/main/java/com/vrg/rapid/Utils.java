package com.vrg.rapid;

import com.vrg.rapid.pb.NodeId;

import java.util.UUID;

/**
 * Utility methods to convert protobuf types
 */
public final class Utils {

    private Utils() {
    }

    static NodeId nodeIdFromUUID(final UUID uuid) {
        return NodeId.newBuilder().setHigh(uuid.getMostSignificantBits())
                                  .setLow(uuid.getLeastSignificantBits()).build();
    }
}
