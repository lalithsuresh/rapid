package com.vrg.rapid;

import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusResponse;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.Phase1aMessage;
import com.vrg.rapid.pb.Phase1bMessage;
import com.vrg.rapid.pb.Phase2aMessage;
import com.vrg.rapid.pb.Phase2bMessage;
import com.vrg.rapid.pb.PreJoinMessage;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import com.vrg.rapid.pb.Response;

import java.util.UUID;

/**
 * Utility methods to convert protobuf types
 */
final class Utils {

    private Utils() {
    }

    static NodeId nodeIdFromUUID(final UUID uuid) {
        return NodeId.newBuilder().setHigh(uuid.getMostSignificantBits())
                                  .setLow(uuid.getLeastSignificantBits()).build();
    }

    /**
     * Helpers to avoid the boilerplate of constructing a new RapidRequest/RapidResponse for
     * every message we want to send out.
     */
    static RapidRequest toRapidRequest(final PreJoinMessage msg) {
        return RapidRequest.newBuilder().setPreJoinMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final JoinMessage msg) {
        return RapidRequest.newBuilder().setJoinMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final BatchedLinkUpdateMessage msg) {
        return RapidRequest.newBuilder().setBatchedLinkUpdateMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final ProbeMessage msg) {
        return RapidRequest.newBuilder().setProbeMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final FastRoundPhase2bMessage msg) {
        return RapidRequest.newBuilder().setFastRoundPhase2BMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase1aMessage msg) {
        return RapidRequest.newBuilder().setPhase1AMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase1bMessage msg) {
        return RapidRequest.newBuilder().setPhase1BMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase2aMessage msg) {
        return RapidRequest.newBuilder().setPhase2AMessage(msg).build();
    }

    static RapidRequest toRapidRequest(final Phase2bMessage msg) {
        return RapidRequest.newBuilder().setPhase2BMessage(msg).build();
    }

    static RapidResponse toRapidResponse(final JoinResponse msg) {
        return RapidResponse.newBuilder().setJoinResponse(msg).build();
    }

    static RapidResponse toRapidResponse(final Response msg) {
        return RapidResponse.newBuilder().setResponse(msg).build();
    }

    static RapidResponse toRapidResponse(final ConsensusResponse msg) {
        return RapidResponse.newBuilder().setConsensusResponse(msg).build();
    }

    static RapidResponse toRapidResponse(final ProbeResponse msg) {
        return RapidResponse.newBuilder().setProbeResponse(msg).build();
    }
}
