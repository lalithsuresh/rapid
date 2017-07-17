package com.vrg.rapid.messaging;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.ConsensusProposalResponse;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import com.vrg.rapid.pb.Response;

/**
 * Represents the sending part of the messaging API
 */
public interface IMessagingClient {
    /**
     * Send a protobuf ProbeMessage to a remote host.
     *
     * @param remote Remote host to send the message to
     * @param probeMessage Probing message for the remote node's failure detector module.
     * @return A future that returns a ProbeResponse if the call was successful.
     */
    ListenableFuture<ProbeResponse> sendProbeMessage(final HostAndPort remote, final ProbeMessage probeMessage);
    /**
     * Create and send a protobuf JoinMessage to a remote host.
     *
     * @param remote Remote host to send the message to
     * @param sender The node sending the join message
     * @return A future that returns a JoinResponse if the call was successful.
     */
    ListenableFuture<JoinResponse> sendJoinMessage(final HostAndPort remote, final HostAndPort sender,
                                                   final NodeId nodeId);
    /**
     * Create and send a protobuf JoinPhase2Message to a remote host.
     *
     * @param remote Remote host to send the message to. This node is expected to initiate LinkUpdate-UP messages.
     * @param msg The JoinMessage for phase two.
     * @return A future that returns a JoinResponse if the call was successful.
     */
    ListenableFuture<JoinResponse> sendJoinPhase2Message(final HostAndPort remote, final JoinMessage msg);

    /**
     * Sends a consensus proposal to a remote node
     *
     * @param remote Remote host to send the message to.
     * @param msg Consensus proposal message
     */
    ListenableFuture<ConsensusProposalResponse> sendConsensusProposal(final HostAndPort remote,
                                                                      final ConsensusProposal msg);

    /**
     * Sends a link update message to a remote node
     *
     * @param remote Remote host to send the message to.
     * @param msg A BatchedLinkUpdateMessage that contains one or more LinkUpdateMessages
     */
    ListenableFuture<Response> sendLinkUpdateMessage(final HostAndPort remote, final BatchedLinkUpdateMessage msg);

    /**
     * Signals to the messaging client that it should cleanup all resources in use.
     */
    void shutdown();
}