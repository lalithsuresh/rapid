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

package com.vrg.rapid;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Simple best-effort broadcaster.
 */
final class UnicastToAllBroadcaster implements IBroadcaster {
    private final IMessagingClient messagingClient;
    private List<Endpoint> recipients = new ArrayList<>();

    UnicastToAllBroadcaster(final IMessagingClient messagingClient) {
        this.messagingClient = messagingClient;
    }

    @Override
    @CanIgnoreReturnValue
    public synchronized List<ListenableFuture<RapidResponse>> broadcast(final RapidRequest msg,
                                                                        final long configurationId) {
        // Randomize the sequence of nodes that will receive a broadcast from this node
        final List<Endpoint> arr = new ArrayList<>(recipients);
        Collections.shuffle(arr, ThreadLocalRandom.current());

        final List<ListenableFuture<RapidResponse>> futures = new ArrayList<>(recipients.size());
        for (final Endpoint recipient: arr) {
            futures.add(messagingClient.sendMessageBestEffort(recipient, msg));
        }
        return futures;
    }

    @Override
    public synchronized void onNodeAdded(final Endpoint node, final Optional<Metadata> metadata) {
        recipients.add(node);
    }

    @Override
    public synchronized void onNodeRemoved(final Endpoint node) {
        recipients.remove(node);
    }
}