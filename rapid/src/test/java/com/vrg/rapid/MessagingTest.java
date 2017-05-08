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
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.ServerInterceptor;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests to drive the messaging sub-system
 */
public class MessagingTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;

    private final int serverPortBase = 1134;
    private static final String localhostIp = "127.0.0.1";
    private final List<RpcServer> rpcServers = new ArrayList<>();
    private final List<MembershipService> services = new ArrayList<>();
    private final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    static {
        // gRPC INFO logs clutter the test output
        Logger.getLogger("io.grpc").setLevel(Level.WARNING);
    }

    @After
    public void cleanup() throws InterruptedException {
        rpcServers.forEach(RpcServer::stopServer);
        rpcServers.clear();
        for (final MembershipService service: services) {
            service.shutdown();
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
        @SuppressWarnings("unused") final RpcServer rpcServer = createAndStartMembershipService(serverAddr);

        final HostAndPort clientAddr = HostAndPort.fromParts(localhostIp, clientPort);
        final RpcClient client = new RpcClient(clientAddr);
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
        final UUID nodeIdentifier = UUID.randomUUID();
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPort);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        @SuppressWarnings("unused") final RpcServer rpcServer = createAndStartMembershipService(serverAddr,
                                            new ArrayList<>(), membershipView);

        // Try with the same host details as the server
        final HostAndPort clientAddr1 = HostAndPort.fromParts(localhostIp, serverPort);
        final RpcClient client1 = new RpcClient(clientAddr1);
        final JoinResponse result1 = client1.sendJoinMessage(serverAddr, clientAddr1, UUID.randomUUID()).get();
        assertNotNull(result1);
        assertEquals(JoinStatusCode.HOSTNAME_ALREADY_IN_RING, result1.getStatusCode());
        assertEquals(K, result1.getHostsCount());
        assertEquals(0, result1.getIdentifiersCount());

        // Try again with a different port, this should fail because we're using the same
        // uuid as the server.
        final int clientPort2 = 1235;
        final HostAndPort clientAddr2 = HostAndPort.fromParts(localhostIp, clientPort2);
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
        final UUID nodeIdentifier = UUID.randomUUID();
        final int numNodes = 1000;
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPortBase);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        for (int i = 1; i < numNodes; i++) {
            membershipView.ringAdd(HostAndPort.fromParts(localhostIp, serverPortBase + i), UUID.randomUUID());
        }
        @SuppressWarnings("unused") final RpcServer rpcServer = createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);

        final int clientPort = serverPortBase - 1;
        final HostAndPort joinerAddr = HostAndPort.fromParts(localhostIp, clientPort);
        final RpcClient joinerClient = new RpcClient(joinerAddr);
        final JoinResponse phaseOneResult = joinerClient.sendJoinMessage(serverAddr,
                                                                         joinerAddr, UUID.randomUUID()).get();
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
        final UUID nodeIdentifier = UUID.randomUUID();
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPortBase);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);

        final int clientPort = serverPortBase - 1;
        final HostAndPort joinerAddress = HostAndPort.fromParts(localhostIp, clientPort);
        final RpcClient joinerRpcClient = new RpcClient(joinerAddress);
        final UUID joinerUuid = UUID.randomUUID();
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
        final UUID nodeIdentifier = UUID.randomUUID();
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPortBase);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr,
                new ArrayList<>(), membershipView);

        final int clientPort = serverPortBase - 1;
        final HostAndPort joinerAddress = HostAndPort.fromParts(localhostIp, clientPort);
        final RpcClient joinerRpcClient = new RpcClient(joinerAddress);
        final UUID joinerUuid = UUID.randomUUID();
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
        final HostAndPort serverAddr1 = HostAndPort.fromParts(localhostIp, serverPortBase);
        final HostAndPort serverAddr2 = HostAndPort.fromParts(localhostIp, serverPortBase + 1);
        final UUID nodeIdentifier1 = UUID.randomUUID();
        final UUID nodeIdentifier2 = UUID.randomUUID();
        final RpcServer rpcServer = new RpcServer(serverAddr2, executorService);
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
        final HostAndPort serverAddr = HostAndPort.fromParts(localhostIp, serverPort);
        final List<ServerInterceptor> interceptors = new ArrayList<>();
        interceptors.add(new ServerDropInterceptors.FixedProbability(1.0));
        final RpcServer rpcServer = createAndStartMembershipService(serverAddr, interceptors);

        final HostAndPort clientAddr = HostAndPort.fromParts(localhostIp, serverPort);
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
     * Create a membership service listenting on serverAddr
     */
    private RpcServer createAndStartMembershipService(final HostAndPort serverAddr)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, UUID.randomUUID());
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView)
                                    .build();
        final RpcServer rpcServer = new RpcServer(serverAddr, executorService);
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
        membershipView.ringAdd(serverAddr, UUID.randomUUID());
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView)
                        .build();
        final RpcServer rpcServer = new RpcServer(serverAddr, executorService);
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
                new MembershipService.Builder(serverAddr, watermarkBuffer, membershipView)
                        .build();
        final RpcServer rpcServer = new RpcServer(serverAddr, executorService);
        rpcServer.setMembershipService(service);
        rpcServer.startServer(interceptors);
        rpcServers.add(rpcServer);
        services.add(service);
        return rpcServer;
    }
}