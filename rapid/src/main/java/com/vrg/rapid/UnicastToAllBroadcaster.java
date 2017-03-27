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
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.ConsensusProposalResponse;
import com.vrg.rapid.pb.Response;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;


/**
 * Simple best-effort broadcaster.
 */
final class UnicastToAllBroadcaster implements IBroadcaster {
    private static final Logger LOG = LoggerFactory.getLogger(UnicastToAllBroadcaster.class);

    private final RpcClient rpcClient;
    private final ExecutorService executorService;

    public UnicastToAllBroadcaster(final RpcClient rpcClient, final ExecutorService executorService) {
        this.rpcClient = rpcClient;
        this.executorService = executorService;
    }

    @Override
    public void broadcast(final List<HostAndPort> recipients, final BatchedLinkUpdateMessage msg) {
        executorService.execute(
            () -> {
                for (final HostAndPort recipient: recipients) {
                    rpcClient.sendLinkUpdateMessage(recipient, msg);
                }
            }
        );
    }

    @Override
    public void broadcast(final List<HostAndPort> recipients, final ConsensusProposal msg) {
        executorService.execute(
            () -> {
                for (final HostAndPort recipient: recipients) {
                    rpcClient.sendConsensusProposal(recipient, msg);
                }
            }
        );
    }
}