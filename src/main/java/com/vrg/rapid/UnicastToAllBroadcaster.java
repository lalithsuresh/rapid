/*
 * Copyright © 2016 - 2017 VMware, Inc. All Rights Reserved.
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

package com.vrg.rapid;

import com.google.common.net.HostAndPort;

import java.util.List;

/**
 * Simple best-effort broadcaster.
 */
public class UnicastToAllBroadcaster implements IBroadcaster {
    private final MessagingClient messagingClient;

    public UnicastToAllBroadcaster(final MessagingClient messagingClient) {
        this.messagingClient = messagingClient;
    }

    @Override
    public void broadcast(final List<HostAndPort> recipients, final LinkUpdateMessage msg) {
        recipients.parallelStream().forEach(remote -> messagingClient.sendLinkUpdateMessage(remote, msg));
    }
}