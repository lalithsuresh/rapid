package com.vrg.rapid.messaging;


import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.MembershipService;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;

import java.io.IOException;

/**
 * Represents the receive part of the messaging API
 */
public interface IMessagingServer {
    /**
     * Receive a protobuf ProbeMessage.
     *
     * @param probeMessage Probing message for the receiving node's failure detector module.
     * @return A future that returns a ProbeResponse if the call was successful.
     */
    ListenableFuture<ProbeResponse> processProbeMessage(final ProbeMessage probeMessage);
    /**
     * Receive a join phase one message
     *
     * @param msg The JoinMessage for phase one.
     * @return A future that returns a JoinResponse if the call was successful.
     */
    ListenableFuture<JoinResponse> processJoinMessage(final JoinMessage msg);
    /**
     * Receive a join phase two message
     *
     * @param msg The JoinMessage for phase two.
     * @return A future that returns a JoinResponse if the call was successful.
     */
    ListenableFuture<JoinResponse> processJoinPhaseTwoMessage(final JoinMessage msg);

    /**
     * Receive a consensus proposal
     *
     * @param msg Consensus proposal message
     */
    void processConsensusProposal(final ConsensusProposal msg);

    /**
     * Receive a link update message
     *
     * @param msg A BatchedLinkUpdateMessage that contains one or more LinkUpdateMessages
     */
    void processLinkUpdateMessage(final BatchedLinkUpdateMessage msg);

    /**
     * Start the server process
     */
    void start() throws IOException;

    /**
     * Signals to the server that it should cleanup all resources in use.
     */
    void shutdown();

    /**
     * The server is initialized in Rapid before the MembershipService itself is ready and initialized (upon
     * a successful join). This call signals to the messaging server that it can start directing messages
     * to a membership service.
     */
    void setMembershipService(final MembershipService service);
}
