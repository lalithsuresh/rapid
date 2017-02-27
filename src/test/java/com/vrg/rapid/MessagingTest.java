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
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.LinkStatus;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests to drive the messaging sub-system
 */
public class MessagingTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;

    private final int serverPortBase = 1234;
    private static final String localhostIp = "127.0.0.1";
    private static final long configurationId = -1;

    /**
     * Single node gets a join request from a peer with non conflicting
     * hostname and UUID
     */
    @Test
    public void joinFirstNode() throws InterruptedException, IOException,
            MembershipView.NodeAlreadyInRingException, ExecutionException {
        final int serverPort = 1234;
        final int clientPort = 1235;
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPort);
        final MembershipService service = createAndStartMembershipService(serverAddr);

        final HostAndPort clientAddr = HostAndPort.fromParts(localhostIp, clientPort);
        final MessagingClient client = new MessagingClient(clientAddr);
        final JoinResponse result = client.sendJoinMessage(serverAddr, clientAddr, UUID.randomUUID()).get();
        assertNotNull(result);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, result.getStatusCode());
        assertEquals(1, result.getHostsCount());
        assertEquals(1, result.getIdentifiersCount());

        service.stopServer();
        service.blockUntilShutdown();
    }

    /**
     * Single node gets a join request from a peer with conflicting
     * hostnames and UUID
     */
    @Test
    public void joinFirstNodeRetryWithErrors()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final int serverPort = 1234;
        final UUID uuid = UUID.randomUUID();
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPort);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, uuid);
        final MembershipService service = createAndStartMembershipService(serverAddr,
                                            new ArrayList<>(), membershipView);

        // Try with the same host details as the server
        final HostAndPort clientAddr1 = HostAndPort.fromParts(localhostIp, serverPort);
        final MessagingClient client1 = new MessagingClient(clientAddr1);
        final JoinResponse result1 = client1.sendJoinMessage(serverAddr, clientAddr1, UUID.randomUUID()).get();
        assertNotNull(result1);
        assertEquals(JoinStatusCode.HOSTNAME_ALREADY_IN_RING, result1.getStatusCode());
        assertEquals(0, result1.getHostsCount());
        assertEquals(0, result1.getIdentifiersCount());

        // Try again with a different port, this should fail because we're using the same
        // uuid as the server.
        final int clientPort2 = 1235;
        final HostAndPort clientAddr2 = HostAndPort.fromParts(localhostIp, clientPort2);
        final MessagingClient client2 = new MessagingClient(clientAddr2);
        final JoinResponse result2 = client2.sendJoinMessage(serverAddr, clientAddr2, uuid).get();
        assertNotNull(result2);
        assertEquals(JoinStatusCode.UUID_ALREADY_IN_RING, result2.getStatusCode());
        assertEquals(0, result2.getHostsCount());
        assertEquals(0, result2.getIdentifiersCount());

        service.stopServer();
        service.blockUntilShutdown();
    }

    /**
     * A node in a cluster gets a join request from a peer with non conflicting
     * hostnames and UUID. Verify the cluster Configuration relayed to
     * the requesting peer.
     */
    @Test
    public void joinWithMultipleNodesCheckConfiguration()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final UUID uuid = UUID.randomUUID();
        final int numNodes = 1000;
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPortBase);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, uuid);
        for (int i = 1; i < numNodes; i++) {
            membershipView.ringAdd(HostAndPort.fromParts(localhostIp, serverPortBase + i), UUID.randomUUID());
        }
        final MembershipService service = createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);

        try {
            final int clientPort = serverPortBase - 1;
            final HostAndPort clientAddr1 = HostAndPort.fromParts(localhostIp, clientPort);
            final MessagingClient client1 = new MessagingClient(clientAddr1);
            final JoinResponse result1 = client1.sendJoinMessage(serverAddr, clientAddr1, UUID.randomUUID()).get();
            assertNotNull(result1);
            assertEquals(JoinStatusCode.SAFE_TO_JOIN, result1.getStatusCode());
            assertEquals(numNodes, result1.getHostsCount());
            assertEquals(numNodes, result1.getIdentifiersCount());

            // Verify that the identifiers and hostnames retrieved at the joining peer
            // can be used to construct an identical membership view object as the
            // seed node that relayed it.
            final List<UUID> identifiersList = result1.getIdentifiersList().stream()
                                                .map(e -> UUID.fromString(e.toStringUtf8()))
                                                .collect(Collectors.toList());
            final List<HostAndPort> hostnameList = result1.getHostsList().stream()
                                                .map(e -> HostAndPort.fromString(e.toStringUtf8()))
                                                .collect(Collectors.toList());

            final long retrievedConfigurationId =
                    MembershipView.Configuration.getConfigurationId(identifiersList,
                                                                    hostnameList);
            assertEquals(membershipView.getCurrentConfigurationId(), retrievedConfigurationId);

            final MembershipView membershipViewJoiningNode = new MembershipView(K, identifiersList, hostnameList);
            assertEquals(membershipView.getMembershipSize(),
                         membershipViewJoiningNode.getMembershipSize());
            assertEquals(membershipView.getCurrentConfigurationId(),
                         membershipViewJoiningNode.getCurrentConfigurationId());

            // We're being a little paranoid here, but verify that the rings look identical
            // between both nodes
            for (int k = 0; k < K; k++) {
                final Iterator seedRingIter = membershipView.viewRing(k).iterator();
                final Iterator joinerRingIter = membershipView.viewRing(k).iterator();
                for (int i = 0; i < numNodes; i++) {
                    assertEquals(seedRingIter.next(), joinerRingIter.next());
                }
            }
        }
        finally {
            service.stopServer();
            service.blockUntilShutdown();
        }
    }


    /**
     * Test bootstrap
     */
    @Test
    public void joinWithMultipleNodesBootstrap()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final UUID uuid = UUID.randomUUID();
        final int numNodes = 1;
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPortBase);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, uuid);
        final MembershipService service = createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);
        MembershipService clientService = null;

        try {
            final int clientPort = serverPortBase - 1;
            final HostAndPort clientAddr1 = HostAndPort.fromParts(localhostIp, clientPort);
            final MessagingClient client1 = new MessagingClient(clientAddr1);
            final UUID clientUuid = UUID.randomUUID();
            final JoinResponse result1 = client1.sendJoinMessage(serverAddr, clientAddr1, clientUuid).get();
            assertNotNull(result1);
            assertEquals(JoinStatusCode.SAFE_TO_JOIN, result1.getStatusCode());
            assertEquals(numNodes, result1.getHostsCount());
            assertEquals(numNodes, result1.getIdentifiersCount());

            // Verify that the identifiers and hostnames retrieved at the joining peer
            // can be used to construct an identical membership view object as the
            // seed node that relayed it.
            final List<UUID> identifiersList = result1.getIdentifiersList().stream()
                    .map(e -> UUID.fromString(e.toStringUtf8()))
                    .collect(Collectors.toList());
            final List<HostAndPort> hostnameList = result1.getHostsList().stream()
                    .map(e -> HostAndPort.fromString(e.toStringUtf8()))
                    .collect(Collectors.toList());

            final MembershipView membershipViewJoiningNode = new MembershipView(K, identifiersList, hostnameList);
            assertEquals(membershipView.getMembershipSize(),
                    membershipViewJoiningNode.getMembershipSize());
            assertEquals(membershipView.getCurrentConfigurationId(),
                    membershipViewJoiningNode.getCurrentConfigurationId());

            clientService = createAndStartMembershipService(clientAddr1,
                    new ArrayList<>(), membershipViewJoiningNode);

            final JoinResponse joinPhase2response =
                    client1.sendJoinPhase2Message(serverAddr, clientAddr1,
                            clientUuid, membershipViewJoiningNode.getCurrentConfigurationId()).get();
            assertNotNull(joinPhase2response);
            assertEquals(JoinStatusCode.SAFE_TO_JOIN, joinPhase2response.getStatusCode());
        }
        finally {
            service.stopServer();
            service.blockUntilShutdown();

            if (clientService != null) {
                clientService.stopServer();
                clientService.blockUntilShutdown();
            }
        }
    }

    /**
     * Test bootstrap with multiple services
     */
    @Test
    public void joinWithMultipleServicesBootstrap()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        List<MembershipService> services = new ArrayList<>();
        MembershipService clientService = null;
        try {
            final int N = 50;
            services = createAndStartMembershipServices(N);

            // This will be the joining node
            final int joinerPort = serverPortBase - 1;
            final HostAndPort joinerAddr = HostAndPort.fromParts(localhostIp, joinerPort);
            final MessagingClient joinerClient = new MessagingClient(joinerAddr);
            final UUID joinerUuid = UUID.randomUUID();
            final HostAndPort seed = HostAndPort.fromParts("127.0.0.1", serverPortBase + 1);
            final JoinResponse joinPhaseOneResult = joinerClient.sendJoinMessage(seed, joinerAddr, joinerUuid).get();
            assertNotNull(joinPhaseOneResult);
            assertEquals(JoinStatusCode.SAFE_TO_JOIN, joinPhaseOneResult.getStatusCode());
            final List<UUID> identifiersList = joinPhaseOneResult.getIdentifiersList().stream()
                    .map(e -> UUID.fromString(e.toStringUtf8()))
                    .collect(Collectors.toList());
            final List<HostAndPort> hostnameList = joinPhaseOneResult.getHostsList().stream()
                    .map(e -> HostAndPort.fromString(e.toStringUtf8()))
                    .collect(Collectors.toList());
            final MembershipView membershipViewJoiningNode = new MembershipView(K, identifiersList, hostnameList);

            // Use the constructed membership service to find the monitors
            clientService = createAndStartMembershipService(joinerAddr,
                    new ArrayList<>(), membershipViewJoiningNode);
            services.add(clientService);
            for (final HostAndPort monitor: membershipViewJoiningNode.expectedMonitorsOf(joinerAddr)) {
                final JoinResponse joinPhase2response =
                        joinerClient.sendJoinPhase2Message(monitor, joinerAddr,
                                joinerUuid, membershipViewJoiningNode.getCurrentConfigurationId()).get();
                assertNotNull(joinPhase2response);
                assertEquals(JoinStatusCode.SAFE_TO_JOIN, joinPhase2response.getStatusCode());
            }

            for (int i = 0; i < N; i++) {
                assertEquals(1, services.get(i).getProposalLog().size());
                assertEquals(1, services.get(i).getProposalLog().get(0).size());
                assertEquals(joinerAddr, services.get(i).getProposalLog().get(0).get(0).hostAndPort);
            }
        } finally {
            shutDownServices(services);
        }
    }

    @Test
    public void droppedMessage() throws InterruptedException,
            IOException, MembershipView.NodeAlreadyInRingException {
        final int serverPort = 1234;
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPort);
        final MembershipService service = createAndStartMembershipService(serverAddr,
                Collections.singletonList(new MessageDropInterceptor()));

        final HostAndPort clientAddr = HostAndPort.fromParts(localhostIp, serverPort);
        final MessagingClient client = new MessagingClient(clientAddr);
        boolean exceptionCaught = false;
        try {
            client.sendLinkUpdateMessage(serverAddr, clientAddr, serverAddr,
                                         LinkStatus.DOWN, configurationId);
        } catch (final StatusRuntimeException e) {
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
        service.stopServer();
        service.blockUntilShutdown();
    }

    private MembershipService createAndStartMembershipService(final HostAndPort serverAddr)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, UUID.randomUUID());
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView)
                                    .setLogProposals(true)
                                    .build();
        service.startServer();
        return service;
    }

    private MembershipService createAndStartMembershipService(final HostAndPort serverAddr,
                                                              final List<ServerInterceptor> interceptors)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, UUID.randomUUID());
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView)
                        .setLogProposals(true)
                        .build();
        service.startServer(interceptors);
        return service;
    }

    private MembershipService createAndStartMembershipService(final HostAndPort serverAddr,
                                                              final List<ServerInterceptor> interceptors,
                                                              final MembershipView membershipView)
            throws IOException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView)
                        .setLogProposals(true)
                        .build();
        service.startServer(interceptors);
        return service;
    }

    private List<MembershipService> createAndStartMembershipServices(final int N) throws IOException {
        final List<MembershipService> services = new ArrayList<>(N);
        for (int i = serverPortBase; i < serverPortBase + N; i++) {
            final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, i);
            services.add(createAndStartMembershipService(serverAddr, Collections.emptyList(), createMembershipView(N)));
        }

        return services;
    }

    private void shutDownServices(final List<MembershipService> services) {
        services.parallelStream().forEach(service -> {
            service.stopServer();
            try {
                service.blockUntilShutdown();
            } catch (final InterruptedException e) {
                fail();
            }
        });
    }

    private MembershipView createMembershipView(final int N) {
        final MembershipView membershipView = new MembershipView(K);
        for (int i = serverPortBase; i < serverPortBase + N; i++) {
            try {
                final HostAndPort hostAndPort = HostAndPort.fromParts(localhostIp, i);
                membershipView.ringAdd(hostAndPort, UUID.nameUUIDFromBytes(hostAndPort.toString().getBytes()));
            } catch (final Exception e){
                fail();
            }
        }
        return membershipView;
    }
}