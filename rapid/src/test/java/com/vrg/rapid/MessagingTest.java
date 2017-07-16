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
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.pb.BatchedLinkUpdateMessage;
import com.vrg.rapid.pb.ConsensusProposal;
import com.vrg.rapid.pb.ConsensusProposalResponse;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.ServerInterceptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
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

    private static final int SERVER_PORT_BASE = 1134;
    private static final String LOCALHOST_IP = "127.0.0.1";
    private final List<RpcServer> rpcServers = new ArrayList<>();
    private final List<MembershipService> services = new ArrayList<>();
    @Nullable private SharedResources resources = null;

    @Before
    public void prepare() throws InterruptedException {
        resources = new SharedResources(HostAndPort.fromParts(LOCALHOST_IP, SERVER_PORT_BASE));
    }

    @After
    public void cleanup() throws InterruptedException {
        rpcServers.forEach(RpcServer::stopServer);
        rpcServers.clear();
        for (final MembershipService service: services) {
            service.shutdown();
        }
        services.clear();
        if (resources != null) {
            resources.shutdown();
        }
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
        final HostAndPort serverAddr = HostAndPort.fromParts(LOCALHOST_IP, serverPort);
        createAndStartMembershipService(serverAddr);

        final HostAndPort clientAddr = HostAndPort.fromParts(LOCALHOST_IP, clientPort);
        final RpcClient client = new RpcClient(clientAddr);
        final JoinResponse result = client.sendJoinMessage(serverAddr, clientAddr,
                                                           Utils.nodeIdFromUUID(UUID.randomUUID())).get();
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
        final NodeId nodeIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
        final HostAndPort serverAddr = HostAndPort.fromParts(LOCALHOST_IP, serverPort);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr, new ArrayList<>(), membershipView);

        // Try with the same host details as the server
        final HostAndPort clientAddr1 = HostAndPort.fromParts(LOCALHOST_IP, serverPort);
        final RpcClient client1 = new RpcClient(clientAddr1);
        final JoinResponse result1 = client1.sendJoinMessage(serverAddr, clientAddr1,
                                                             Utils.nodeIdFromUUID(UUID.randomUUID())).get();
        assertNotNull(result1);
        assertEquals(JoinStatusCode.HOSTNAME_ALREADY_IN_RING, result1.getStatusCode());
        assertEquals(K, result1.getHostsCount());
        assertEquals(0, result1.getIdentifiersCount());

        // Try again with a different port, this should fail because we're using the same
        // uuid as the server.
        final int clientPort2 = 1235;
        final HostAndPort clientAddr2 = HostAndPort.fromParts(LOCALHOST_IP, clientPort2);
        final RpcClient client2 = new RpcClient(clientAddr2);
        final JoinResponse result2 = client2.sendJoinMessage(serverAddr, clientAddr2, nodeIdentifier).get();
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
        final NodeId nodeIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
        final int numNodes = 1000;
        final HostAndPort serverAddr = HostAndPort.fromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        for (int i = 1; i < numNodes; i++) {
            membershipView.ringAdd(HostAndPort.fromParts(LOCALHOST_IP, SERVER_PORT_BASE + i),
                                   Utils.nodeIdFromUUID(UUID.randomUUID()));
        }
        createAndStartMembershipService(serverAddr, new ArrayList<>(), membershipView);

        final int clientPort = SERVER_PORT_BASE - 1;
        final HostAndPort joinerAddr = HostAndPort.fromParts(LOCALHOST_IP, clientPort);
        final RpcClient joinerClient = new RpcClient(joinerAddr);
        final JoinResponse phaseOneResult = joinerClient.sendJoinMessage(serverAddr,
                                                                         joinerAddr,
                                                                         Utils.nodeIdFromUUID(UUID.randomUUID())).get();
        assertNotNull(phaseOneResult);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, phaseOneResult.getStatusCode());
        assertEquals(K, phaseOneResult.getHostsCount()); // this is the monitor list

        // Verify that the monitors retrieved from the seed are the same
        final List<HostAndPort> hostsAtClient = phaseOneResult.getHostsList().stream()
                                            .map(HostAndPort::fromString)
                                            .collect(Collectors.toList());
        final List<HostAndPort> monitorsOriginal = membershipView.getExpectedMonitorsOf(joinerAddr);

        final Iterator iter1 = hostsAtClient.iterator();
        final Iterator iter2 = monitorsOriginal.iterator();
        for (int i = 0; i < hostsAtClient.size(); i++) {
            assertEquals(iter1.next(), iter2.next());
        }
    }

    /**
     * Test bootstrap with a single node.
     */
    @Test
    public void joinWithSingleNodeBootstrap()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final NodeId nodeIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
        final HostAndPort serverAddr = HostAndPort.fromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);

        final int clientPort = SERVER_PORT_BASE - 1;
        final HostAndPort joinerAddress = HostAndPort.fromParts(LOCALHOST_IP, clientPort);
        final RpcClient joinerRpcClient = new RpcClient(joinerAddress);
        final NodeId joinerUuid = Utils.nodeIdFromUUID(UUID.randomUUID());
        final JoinResponse response = joinerRpcClient.sendJoinMessage(serverAddr, joinerAddress, joinerUuid).get();
        assertNotNull(response);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
        assertEquals(K, response.getHostsCount());
        assertEquals(response.getConfigurationId(), membershipView.getCurrentConfigurationId());

        // Verify that the hostnames retrieved at the joining peer
        // matches that of the seed node.
        final List<HostAndPort> monitorList = response.getHostsList().stream()
                .map(HostAndPort::fromString)
                .collect(Collectors.toList());

        final Iterator<HostAndPort> iterJoiner = monitorList.iterator();
        final Iterator<HostAndPort> iterSeed = membershipView.getExpectedMonitorsOf(joinerAddress).iterator();
        for (int i = 0; i < K; i++) {
            assertEquals(iterJoiner.next(), iterSeed.next());
        }
    }

    /**
     * Test probing code path.
     */
    @Test
    public void bootstrapAndThenProbeTest()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final NodeId nodeIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
        final HostAndPort serverAddr = HostAndPort.fromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);

        final int clientPort = SERVER_PORT_BASE - 1;
        final HostAndPort joinerAddress = HostAndPort.fromParts(LOCALHOST_IP, clientPort);
        final RpcClient joinerRpcClient = new RpcClient(joinerAddress);
        final NodeId joinerUuid = Utils.nodeIdFromUUID(UUID.randomUUID());
        final JoinResponse response = joinerRpcClient.sendJoinMessage(serverAddr, joinerAddress, joinerUuid).get();
        assertNotNull(response);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
        assertEquals(K, response.getHostsCount());
        assertEquals(response.getConfigurationId(), membershipView.getCurrentConfigurationId());

        // Verify that the hostnames retrieved at the joining peer
        // matches that of the seed node.
        final List<HostAndPort> monitorList = response.getHostsList().stream()
                .map(HostAndPort::fromString)
                .collect(Collectors.toList());

        final Iterator<HostAndPort> iterJoiner = monitorList.iterator();
        final Iterator<HostAndPort> iterSeed = membershipView.getExpectedMonitorsOf(joinerAddress).iterator();
        for (int i = 0; i < K; i++) {
            assertEquals(iterJoiner.next(), iterSeed.next());
        }

        final ProbeResponse probeResponse = joinerRpcClient.sendProbeMessage(serverAddr,
                                                                             ProbeMessage.getDefaultInstance()).get();
        assertNotNull(probeResponse);
        assertEquals(NodeStatus.OK, probeResponse.getStatus());
    }


    /**
     * When a joining node has not yet received the join-confirmation and has not bootstrapped its membership-service,
     * other nodes in the cluster may try to probe it (because they already took part in the consensus decision).
     * This test sets up such a case where there is only an RpcServer running that nodes are attempting to probe.
     */
    @Test
    public void probeBeforeBootstrapTest()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final HostAndPort serverAddr1 = HostAndPort.fromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final HostAndPort serverAddr2 = HostAndPort.fromParts(LOCALHOST_IP, SERVER_PORT_BASE + 1);
        final NodeId nodeIdentifier1 = Utils.nodeIdFromUUID(UUID.randomUUID());
        final NodeId nodeIdentifier2 = Utils.nodeIdFromUUID(UUID.randomUUID());
        final RpcServer rpcServer = new RpcServer(serverAddr2, resources);
        rpcServer.startServer();
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr1, nodeIdentifier1);
        membershipView.ringAdd(serverAddr2, nodeIdentifier2); // This causes server1 to monitor server2
        createAndStartMembershipService(serverAddr1,
                new ArrayList<>(), membershipView);

        // While the above drives our failure detector logic, we explicitly test with a probe call
        // to make sure we get a BOOTSTRAPPING response from the RpcServer listening on serverAddr2.
        final RpcClient joinerRpcClient = new RpcClient(serverAddr2);
        final ProbeResponse probeResponse1 = joinerRpcClient.sendProbeMessage(serverAddr1,
                                                                             ProbeMessage.getDefaultInstance()).get();
        assertEquals(NodeStatus.OK, probeResponse1.getStatus());
        final ProbeResponse probeResponse2 = joinerRpcClient.sendProbeMessage(serverAddr2,
                ProbeMessage.getDefaultInstance()).get();
        assertEquals(NodeStatus.BOOTSTRAPPING, probeResponse2.getStatus());
    }


    /**
     * Test to ensure that injecting message drops works.
     */
    @Test
    public void droppedMessage() throws InterruptedException,
            IOException, MembershipView.NodeAlreadyInRingException {
        final int serverPort = 1234;
        final HostAndPort serverAddr = HostAndPort.fromParts(LOCALHOST_IP, serverPort);
        final List<ServerInterceptor> interceptors = new ArrayList<>();
        interceptors.add(new ServerDropInterceptors.FixedProbability(1.0));
        createAndStartMembershipService(serverAddr, interceptors);

        final HostAndPort clientAddr = HostAndPort.fromParts(LOCALHOST_IP, serverPort);
        final RpcClient client = new RpcClient(clientAddr);
        boolean exceptionCaught = false;
        try {
            client.sendProbeMessage(serverAddr, ProbeMessage.getDefaultInstance()).get();
        } catch (final ExecutionException e) {
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
    }

    /**
     * Tests our broadcaster to make sure it receives responses from all nodes it sends messages to.
     */
    @Test
    public void broadcasterTest() throws IOException, ExecutionException, InterruptedException {
        final int N = 100;
        final List<HostAndPort> hostList = new ArrayList<>(N);
        final int serverPort = 1234;
        for (int i = 0; i < N; i++) {
            final HostAndPort serverAddr = HostAndPort.fromParts(LOCALHOST_IP, serverPort + i + 1);
            createAndStartMembershipService(serverAddr);
            hostList.add(serverAddr);
        }
        final HostAndPort clientAddr = HostAndPort.fromParts(LOCALHOST_IP, serverPort);
        final RpcClient client = new RpcClient(clientAddr);
        final UnicastToAllBroadcaster broadcaster = new UnicastToAllBroadcaster(client);
        broadcaster.setMembership(hostList);
        for (int i = 0; i < 10; i++) {
            final List<ListenableFuture<ConsensusProposalResponse>> futures =
                    broadcaster.broadcast(ConsensusProposal.getDefaultInstance());
            for (final ListenableFuture<ConsensusProposalResponse> future : futures) {
                assertNotNull(future);
                final ConsensusProposalResponse response = future.get();
                assertNotNull(response);
            }
        }
    }

    /**
     * Tests all RpcClient request types to an endpoint that does not exist, checking if all calls fail.
     */
    @Test
    public void rpcClientErrorHandling() throws InterruptedException {
        final int basePort = 1234;
        final HostAndPort clientAddr = HostAndPort.fromParts(LOCALHOST_IP, basePort);
        final HostAndPort dst = HostAndPort.fromParts(LOCALHOST_IP, 4321);
        final RpcClient client = new RpcClient(clientAddr);
        try {
            client.sendProbeMessage(dst, ProbeMessage.getDefaultInstance()).get();
            fail("sendProbeMessage did not throw an exception");
        } catch (final ExecutionException ignored) {
        }
        try {
            client.sendJoinMessage(dst, clientAddr, Utils.nodeIdFromUUID(UUID.randomUUID())).get();
            fail("sendJoinMessage did not throw an exception");
        } catch (final ExecutionException ignored) {
        }
        try {
            client.sendJoinPhase2Message(dst, JoinMessage.getDefaultInstance()).get();
            fail("sendJoinPhase2Message did not throw an exception");
        } catch (final ExecutionException ignored) {
        }
        try {
            client.sendLinkUpdateMessage(dst, BatchedLinkUpdateMessage.getDefaultInstance()).get();
            fail("sendLinkUpdateMessage did not throw an exception");
        } catch (final ExecutionException ignored) {
        }
        try {
            client.sendConsensusProposal(dst, ConsensusProposal.getDefaultInstance()).get();
            fail("sendConsensusProposal did not throw an exception");
        } catch (final ExecutionException ignored) {
        }
    }


    /**
     * Create a membership service listenting on serverAddr
     */
    private RpcServer createAndStartMembershipService(final HostAndPort serverAddr)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView, resources)
                                    .build();
        final RpcServer rpcServer = new RpcServer(serverAddr, resources);
        rpcServer.setMembershipService(service);
        rpcServer.startServer();
        rpcServers.add(rpcServer);
        services.add(service);
        return rpcServer;
    }

    /**
     * Create a membership service listenting on serverAddr that uses a list of server interceptors.
     */
    private RpcServer createAndStartMembershipService(final HostAndPort serverAddr,
                                                      final List<ServerInterceptor> interceptors)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView, resources)
                        .build();
        final RpcServer rpcServer = new RpcServer(serverAddr, resources);
        rpcServer.setMembershipService(service);
        rpcServer.startServer(interceptors);
        rpcServers.add(rpcServer);
        services.add(service);
        return rpcServer;
    }

    /**
     * Create a membership service listening on serverAddr, with a supplied membershipView and server interceptors.
     */
    private RpcServer createAndStartMembershipService(final HostAndPort serverAddr,
                                                      final List<ServerInterceptor> interceptors,
                                                      final MembershipView membershipView)
            throws IOException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView, resources)
                        .build();
        final RpcServer rpcServer = new RpcServer(serverAddr, resources);
        rpcServer.setMembershipService(service);
        rpcServer.startServer(interceptors);
        rpcServers.add(rpcServer);
        services.add(service);
        return rpcServer;
    }
}