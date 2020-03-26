/*
 * Copyright © 2016 - 2020 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
