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
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The public API for Rapid.
 */
public class Cluster {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;
    private static final int RETRIES = 5;
    private final MembershipService membershipService;
    private final RpcServer rpcServer;

    private Cluster(final RpcServer rpcServer, final MembershipService membershipService) {
        this.membershipService = membershipService;
        this.rpcServer = rpcServer;
    }

    /**
     * Joins an existing cluster, using {@code seedAddress} to bootstrap.
     *
     * @param seedAddress Seed node for the bootstrap protocol
     * @param listenAddress Address to bind to after successful bootstrap
     * @throws IOException Thrown if we cannot successfully start a server
     */
    public static Cluster join(final HostAndPort seedAddress,
                     final HostAndPort listenAddress) throws IOException, ExecutionException, InterruptedException {
        Objects.requireNonNull(seedAddress);
        Objects.requireNonNull(listenAddress);

        for (int attempt = 0; attempt < RETRIES; attempt++) {

            // First, get the configuration ID and the monitors to contact from the seed node.
            final RpcClient joinerClient = new RpcClient(listenAddress);
            final UUID currentIdentifier = UUID.randomUUID();  // the logical identifier of the host
            final JoinResponse joinPhaseOneResult = joinerClient.sendJoinMessage(seedAddress,
                    listenAddress, currentIdentifier).get();
            assert joinPhaseOneResult != null;

            // TODO: need to handle other cases
            if (!joinPhaseOneResult.getStatusCode().equals(JoinStatusCode.SAFE_TO_JOIN)) {
                throw new RuntimeException(joinPhaseOneResult.getStatusCode().toString());
            }

            // We have the list of monitors. Now contact them as part of phase 2.
            final List<HostAndPort> monitorList = joinPhaseOneResult.getHostsList().stream()
                    .map(e -> HostAndPort.fromString(e.toStringUtf8()))
                    .collect(Collectors.toList());

            int ringNumber = 0;
            final List<ListenableFuture<JoinResponse>> futures = new ArrayList<>();
            for (final HostAndPort monitor : monitorList) {
                futures.add(joinerClient.sendJoinPhase2Message(monitor, listenAddress, currentIdentifier, ringNumber,
                        joinPhaseOneResult.getConfigurationId()));
                ringNumber++;
            }

            // The returned list of responses must contain the full configuration (hosts and identifiers) we just
            // joined. Else, there's an error and we throw an exception.

            final List<JoinResponse> responses = Futures.allAsList(futures).get();
            for (final JoinResponse response : responses) {
                if (response.getStatusCode() == JoinStatusCode.SAFE_TO_JOIN
                        && response.getConfigurationId() != joinPhaseOneResult.getConfigurationId()) {

                    // Safe to proceed. Extract the list of hosts and identifiers from the message,
                    // assemble a MembershipService object and start an RpcServer.
                    final List<HostAndPort> allHosts = response.getHostsList().stream()
                            .map(e -> HostAndPort.fromString(e.toStringUtf8()))
                            .collect(Collectors.toList());
                    final List<UUID> identifiersSeen = response.getIdentifiersList().stream()
                            .map(e -> UUID.fromString(e.toStringUtf8()))
                            .collect(Collectors.toList());

                    assert identifiersSeen.size() > 0;
                    assert allHosts.size() > 0;
                    final MembershipView membershipViewFinal = new MembershipView(K, identifiersSeen, allHosts);
                    final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
                    final MembershipService membershipService = new MembershipService.Builder(listenAddress,
                            watermarkBuffer,
                            membershipViewFinal)
                            .setLogProposals(true)
                            .build();
                    final RpcServer server = new RpcServer(listenAddress, membershipService);
                    server.startServer();
                    return new Cluster(server, membershipService);
                }
            }
        }
        // TODO: need to handle retries before giving up
        throw new RuntimeException("Join attempt unsuccessful");
    }

    /**
     * Start a cluster without joining. Required to bootstrap a seed node.
     *
     * @param listenAddress Address to bind to after successful bootstrap
     * @throws IOException Thrown if we cannot successfully start a server
     */
    public static Cluster start(final HostAndPort listenAddress) throws IOException {
        Objects.requireNonNull(listenAddress);
        final UUID currentIdentifier = UUID.randomUUID();
        final MembershipView membershipView = new MembershipView(K, Collections.singletonList(currentIdentifier),
                                                                    Collections.singletonList(listenAddress));
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipService membershipService = new MembershipService.Builder(listenAddress, watermarkBuffer,
                                                                                  membershipView)
                                                .setLogProposals(true)
                                                .build();
        final RpcServer rpcServer = new RpcServer(listenAddress, membershipService);
        rpcServer.startServer();
        return new Cluster(rpcServer, membershipService);
    }

    /**
     * Returns the list of hosts currently in the membership set.
     *
     * @return list of hosts in the membership set
     */
    public List<HostAndPort> getMemberlist() {
        return membershipService.getMembershipView();
    }

    /**
     * Shutdown the RpcServer
     *
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        // TODO: this should probably be a "leave" method
        rpcServer.stopServer();
        rpcServer.blockUntilShutdown();
    }
}
