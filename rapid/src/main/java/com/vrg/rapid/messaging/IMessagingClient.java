package com.vrg.rapid.messaging;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the sending part of the messaging API
 */
public interface IMessagingClient {
    /**
     * Send a message to a remote node with re-transmissions if necessary
     *
     * @param remote Remote host to send the message to
     * @param msg Message to send
     * @return A future that returns a RapidResponse if the call was successful.
     */
    @CanIgnoreReturnValue
    ListenableFuture<RapidResponse> sendMessage(final Endpoint remote, final RapidRequest msg);

    /**
     * Send a message to a remote node with best-effort guarantees
     *
     * @param remote Remote host to send the message to
     * @param msg Message to send
     * @return A future that returns a RapidResponse if the call was successful.
     */
    @CanIgnoreReturnValue
    ListenableFuture<RapidResponse> sendMessageBestEffort(final Endpoint remote, final RapidRequest msg);

    /**
     * Signals to the messaging client that it should cleanup all resources in use.
     */
    void shutdown();
}