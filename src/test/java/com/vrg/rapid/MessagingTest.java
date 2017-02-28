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
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
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
    private final List<MembershipService> services = new ArrayList<>();

    @After
    public void cleanup() throws InterruptedException {
        for (final MembershipService service: services) {
            service.stopServer();
            service.blockUntilShutdown();
        }
        services.clear();
    }

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
        assertEquals(K, result.getHostsCount());
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

        final int clientPort = serverPortBase - 1;
        final HostAndPort joinerAddr = HostAndPort.fromParts(localhostIp, clientPort);
        final MessagingClient joinerClient = new MessagingClient(joinerAddr);
        final JoinResponse result1 = joinerClient.sendJoinMessage(serverAddr, joinerAddr, UUID.randomUUID()).get();
        assertNotNull(result1);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, result1.getStatusCode());
        assertEquals(K, result1.getHostsCount());

        // Verify that the monitors retrieved from the seed are the same
        final List<HostAndPort> monitorsAtClient = result1.getHostsList().stream()
                                            .map(e -> HostAndPort.fromString(e.toStringUtf8()))
                                            .collect(Collectors.toList());

        final List<HostAndPort> monitorsOriginal = membershipView.expectedMonitorsOf(joinerAddr);
        assertEquals(monitorsAtClient.size(), monitorsOriginal.size());

        final Iterator iter1 = monitorsAtClient.iterator();
        final Iterator iter2 = monitorsOriginal.iterator();
        for (int i = 0; i < monitorsAtClient.size(); i++) {
            assertEquals(iter1.next(), iter2.next());
        }
    }


    /**
     * Test bootstrap
     */
    @Test
    public void joinWithMultipleNodesBootstrap()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final UUID uuid = UUID.randomUUID();
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPortBase);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, uuid);
        createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);

        final int clientPort = serverPortBase - 1;
        final HostAndPort clientAddr1 = HostAndPort.fromParts(localhostIp, clientPort);
        final MessagingClient client1 = new MessagingClient(clientAddr1);
        final UUID clientUuid = UUID.randomUUID();
        final JoinResponse result1 = client1.sendJoinMessage(serverAddr, clientAddr1, clientUuid).get();
        assertNotNull(result1);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, result1.getStatusCode());
        assertEquals(K, result1.getHostsCount());

        // Verify that the identifiers and hostnames retrieved at the joining peer
        // can be used to construct an identical membership view object as the
        // seed node that relayed it.
        final List<HostAndPort> hostnameList = result1.getHostsList().stream()
                .map(e -> HostAndPort.fromString(e.toStringUtf8()))
                .collect(Collectors.toList());

        for (final HostAndPort host: hostnameList) {
            final JoinResponse joinPhase2response =
                    client1.sendJoinPhase2Message(host, clientAddr1, clientUuid).get();
            assertNotNull(joinPhase2response);
            assertEquals(JoinStatusCode.SAFE_TO_JOIN, joinPhase2response.getStatusCode());
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
        services.add(service);
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
        services.add(service);
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
        services.add(service);
        return service;
    }

    private List<MembershipService> createAndStartMembershipServices(final int N) throws IOException {
        for (int i = serverPortBase; i < serverPortBase + N; i++) {
            final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, i);
            services.add(createAndStartMembershipService(serverAddr, Collections.emptyList(), createMembershipView(N)));
        }

        return services;
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