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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.Response;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Simple best-effort broadcaster.
 */
final class UnicastToAllBroadcaster implements IBroadcaster {
    private static final Logger LOG = LoggerFactory.getLogger(UnicastToAllBroadcaster.class);

    private final RpcClient rpcClient;

    public UnicastToAllBroadcaster(final RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public void broadcast(final List<HostAndPort> recipients, final BatchedLinkUpdateMessage msg) {
        final List<ListenableFuture<Response>> list = new ArrayList<>();
        for (final HostAndPort recipient: recipients) {
            list.add(rpcClient.sendLinkUpdateMessage(recipient, msg));
        }

        try {
            Futures.allAsList(list).get();
        } catch (final InterruptedException | ExecutionException | StatusRuntimeException e) {
            LOG.error("Broadcast returned an error {}, {}", recipients, e.getCause());
        }
    }
}