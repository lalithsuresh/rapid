package com.vrg.rapid.messaging;


import com.vrg.rapid.MembershipService;

import java.io.IOException;

/**
 * Represents the receive part of the messaging API
 */
public interface IMessagingServer {
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
