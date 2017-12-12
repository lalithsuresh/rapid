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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.messaging.IMessagingServer;
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.messaging.impl.GrpcServer;
import com.vrg.rapid.messaging.impl.NettyDirectTcpClient;
import com.vrg.rapid.messaging.impl.NettyDirectTcpServer;
import com.vrg.rapid.monitoring.impl.PingPongFailureDetector;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.PreJoinMessage;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
    private final List<IMessagingServer> rpcServers = new ArrayList<>();
    private final List<MembershipService> services = new ArrayList<>();
    @Nullable private SharedResources resources = null;

    @Before
    public void prepare() throws InterruptedException {
        resources = new SharedResources(Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE));
    }

    @After
    public void cleanup() throws InterruptedException {
        rpcServers.forEach(IMessagingServer::shutdown);
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
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        createAndStartMembershipService(serverAddr);

        final Endpoint clientAddr = Utils.hostFromParts(LOCALHOST_IP, clientPort);
        final GrpcClient client = new GrpcClient(clientAddr);
        final JoinResponse result = sendPreJoinMessage(client, serverAddr, clientAddr,
                                                       Utils.nodeIdFromUUID(UUID.randomUUID()));
        assertNotNull(result);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, result.getStatusCode());
        assertEquals(K, result.getEndpointsCount());
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
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr, membershipView);

        // Try with the same host details as the server
        final Endpoint clientAddr1 = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        final GrpcClient client1 = new GrpcClient(clientAddr1);
        final JoinResponse result1 = sendPreJoinMessage(client1, serverAddr, clientAddr1,
                                                             Utils.nodeIdFromUUID(UUID.randomUUID()));
        assertNotNull(result1);
        assertEquals(JoinStatusCode.HOSTNAME_ALREADY_IN_RING, result1.getStatusCode());
        assertEquals(K, result1.getEndpointsCount());
        assertEquals(0, result1.getIdentifiersCount());

        // Try again with a different port, this should fail because we're using the same
        // uuid as the server.
        final int clientPort2 = 1235;
        final Endpoint clientAddr2 = Utils.hostFromParts(LOCALHOST_IP, clientPort2);
        final GrpcClient client2 = new GrpcClient(clientAddr2);
        final JoinResponse result2 = sendPreJoinMessage(client2, serverAddr, clientAddr2, nodeIdentifier);
        assertNotNull(result2);
        assertEquals(JoinStatusCode.UUID_ALREADY_IN_RING, result2.getStatusCode());
        assertEquals(0, result2.getEndpointsCount());
        assertEquals(0, result2.getIdentifiersCount());
    }

    /**
     * A node in a cluster gets a join request from a peer with non conflicting
     * hostnames and UUID. Verify the cluster Settings relayed to
     * the requesting peer.
     */
    @Test
    public void joinWithMultipleNodesCheckConfiguration()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final NodeId nodeIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
        final int numNodes = 1000;
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        for (int i = 1; i < numNodes; i++) {
            membershipView.ringAdd(Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE + i),
                                   Utils.nodeIdFromUUID(UUID.randomUUID()));
        }
        createAndStartMembershipService(serverAddr, membershipView);

        final int clientPort = SERVER_PORT_BASE - 1;
        final Endpoint joinerAddr = Utils.hostFromParts(LOCALHOST_IP, clientPort);
        final GrpcClient joinerClient = new GrpcClient(joinerAddr);
        final JoinResponse phaseOneResult = sendPreJoinMessage(joinerClient, serverAddr, joinerAddr,
                                                               Utils.nodeIdFromUUID(UUID.randomUUID()));
        assertNotNull(phaseOneResult);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, phaseOneResult.getStatusCode());
        assertEquals(K, phaseOneResult.getEndpointsCount()); // this is the monitor list

        // Verify that the monitors retrieved from the seed are the same
        final List<Endpoint> hostsAtClient = phaseOneResult.getEndpointsList();
        final List<Endpoint> monitorsOriginal = membershipView.getExpectedMonitorsOf(joinerAddr);

        final Iterator iter1 = hostsAtClient.iterator();
        final Iterator iter2 = monitorsOriginal.iterator();
        for (int i = 0; i < hostsAtClient.size(); i++) {
            assertEquals(iter1.next(), iter2.next());
        }
    }

    /**
     * A node in a cluster gets a join request from a peer that is already part of the membership.
     * If the joiner is in the current configuration, it should get back the full configuration.
     */
    @Test
    public void joinWithMultipleNodesCheckRace()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        // Initialize 10 node cluster
        final int numNodes = 10;
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        for (int i = 0; i < numNodes; i++) {
            final MembershipView mview = new MembershipView(K);
            for (int j = 0; j < numNodes; j++) {
                mview.ringAdd(Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE + j),
                        Utils.nodeIdFromUUID(new UUID(0, j)));
            }
            createAndStartMembershipService(Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE + i), mview);
        }

        // Join protocol starts here
        final int clientPort = SERVER_PORT_BASE - 1;
        final Endpoint joinerAddr = Utils.hostFromParts(LOCALHOST_IP, clientPort);
        final GrpcClient joinerClient = new GrpcClient(joinerAddr);
        final NodeId uuid = Utils.nodeIdFromUUID(UUID.randomUUID());
        final JoinResponse phaseOneResult = sendPreJoinMessage(joinerClient, serverAddr, joinerAddr, uuid);

        assertNotNull(phaseOneResult);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, phaseOneResult.getStatusCode());
        assertEquals(K, phaseOneResult.getEndpointsCount()); // this is the monitor list

        // Verify that the monitors retrieved from the seed are the same
        final List<Endpoint> hostsAtClient = phaseOneResult.getEndpointsList();
        final Map<Endpoint, List<Integer>> ringNumbersPerMonitor = new HashMap<>(K);

        // Batch together requests to the same node.
        int ringNumber = 0;
        for (final Endpoint monitor: hostsAtClient) {
            ringNumbersPerMonitor.computeIfAbsent(monitor, k -> new ArrayList<>()).add(ringNumber);
            ringNumber++;
        }

        // Try #1: successfully join here.
        final List<ListenableFuture<RapidResponse>> responseFutures = new ArrayList<>();
        for (final Map.Entry<Endpoint, List<Integer>> entry: ringNumbersPerMonitor.entrySet()) {
            final RapidRequest msg = Utils.toRapidRequest(JoinMessage.newBuilder()
                    .setSender(joinerAddr)
                    .setNodeId(uuid)
                    .setConfigurationId(phaseOneResult.getConfigurationId())
                    .addAllRingNumber(entry.getValue()).build());
            final ListenableFuture<RapidResponse> call = joinerClient.sendMessage(entry.getKey(), msg);
            responseFutures.add(call);
            Futures.addCallback(call, new ResponseCallback());
        }
        final List<JoinResponse> joinResponses = Futures.successfulAsList(responseFutures).get()
                .stream().filter(Objects::nonNull).map(RapidResponse::getJoinResponse).collect(Collectors.toList());
        assertEquals(ringNumbersPerMonitor.size(), joinResponses.size());

        for (final JoinResponse response: joinResponses) {
            assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
        }

        // Try #2. Should get back the full configuration from all nodes.
        final List<ListenableFuture<RapidResponse>> retryFutures = new ArrayList<>();
        for (final Map.Entry<Endpoint, List<Integer>> entry: ringNumbersPerMonitor.entrySet()) {
            final RapidRequest msg = Utils.toRapidRequest(JoinMessage.newBuilder()
                    .setSender(joinerAddr)
                    .setNodeId(uuid)
                    .setConfigurationId(phaseOneResult.getConfigurationId())
                    .addAllRingNumber(entry.getValue()).build());
            final ListenableFuture<RapidResponse> call = joinerClient.sendMessage(entry.getKey(), msg);
            retryFutures.add(call);
        }

        final List<JoinResponse> retriedJoinResponses = Futures.successfulAsList(retryFutures).get()
                .stream().map(RapidResponse::getJoinResponse).collect(Collectors.toList());
        assertEquals(ringNumbersPerMonitor.size(), retriedJoinResponses.size());

        for (final JoinResponse response: retriedJoinResponses) {
            assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
            assertEquals(numNodes + 1, response.getEndpointsCount());
        }
    }

    /**
     * Test bootstrap with a single node.
     */
    @Test
    public void joinWithSingleNodeBootstrap()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final NodeId nodeIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr, membershipView);

        final int clientPort = SERVER_PORT_BASE - 1;
        final Endpoint joinerAddress = Utils.hostFromParts(LOCALHOST_IP, clientPort);
        final IMessagingClient messagingClient = new GrpcClient(joinerAddress);
        final NodeId joinerUuid = Utils.nodeIdFromUUID(UUID.randomUUID());
        final JoinResponse response = sendPreJoinMessage(messagingClient, serverAddr, joinerAddress, joinerUuid);
        assertNotNull(response);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
        assertEquals(K, response.getEndpointsCount());
        assertEquals(response.getConfigurationId(), membershipView.getCurrentConfigurationId());

        // Verify that the hostnames retrieved at the joining peer
        // matches that of the seed node.
        final List<Endpoint> monitorList = response.getEndpointsList();

        final Iterator<Endpoint> iterJoiner = monitorList.iterator();
        final Iterator<Endpoint> iterSeed = membershipView.getExpectedMonitorsOf(joinerAddress).iterator();
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
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr, membershipView);

        final int clientPort = SERVER_PORT_BASE - 1;
        final Endpoint joinerAddress = Utils.hostFromParts(LOCALHOST_IP, clientPort);
        final IMessagingClient messagingClient = new GrpcClient(joinerAddress);
        final NodeId joinerUuid = Utils.nodeIdFromUUID(UUID.randomUUID());
        final JoinResponse response = sendPreJoinMessage(messagingClient, serverAddr, joinerAddress, joinerUuid);
        assertNotNull(response);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
        assertEquals(K, response.getEndpointsCount());
        assertEquals(response.getConfigurationId(), membershipView.getCurrentConfigurationId());

        // Verify that the hostnames retrieved at the joining peer
        // matches that of the seed node.
        final List<Endpoint> monitorList = response.getEndpointsList();

        final Iterator<Endpoint> iterJoiner = monitorList.iterator();
        final Iterator<Endpoint> iterSeed = membershipView.getExpectedMonitorsOf(joinerAddress).iterator();
        for (int i = 0; i < K; i++) {
            assertEquals(iterJoiner.next(), iterSeed.next());
        }

        final RapidResponse probeResponse = messagingClient.sendMessage(serverAddr,
                Utils.toRapidRequest(ProbeMessage.getDefaultInstance())).get();
        assertNotNull(probeResponse);
        assertEquals(NodeStatus.OK, probeResponse.getProbeResponse().getStatus());
    }


    /**
     * When a joining node has not yet received the join-confirmation and has not bootstrapped its membership-service,
     * other nodes in the cluster may try to probe it (because they already took part in the consensus decision).
     * This test sets up such a case where there is only an RpcServer running that nodes are attempting to probe.
     */
    @Test
    public void probeBeforeBootstrapTest()
            throws InterruptedException, IOException, MembershipView.NodeAlreadyInRingException, ExecutionException {
        final Endpoint serverAddr1 = Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE);
        final Endpoint serverAddr2 = Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE + 1);
        final NodeId nodeIdentifier1 = Utils.nodeIdFromUUID(UUID.randomUUID());
        final NodeId nodeIdentifier2 = Utils.nodeIdFromUUID(UUID.randomUUID());
        final IMessagingServer rpcServer = new GrpcServer(serverAddr2, resources, false);
        rpcServer.start();
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr1, nodeIdentifier1);
        membershipView.ringAdd(serverAddr2, nodeIdentifier2); // This causes server1 to monitor server2
        createAndStartMembershipService(serverAddr1, membershipView);
        // While the above drives our failure detector logic, we explicitly test with a probe call
        // to make sure we get a BOOTSTRAPPING response from the RpcServer listening on serverAddr2.
        final GrpcClient joinerRpcClient = new GrpcClient(serverAddr2);
        final RapidResponse probeResponse1 = joinerRpcClient.sendMessage(serverAddr1,
                Utils.toRapidRequest(ProbeMessage.getDefaultInstance())).get();

        assertEquals(NodeStatus.OK, probeResponse1.getProbeResponse().getStatus());
        final RapidResponse probeResponse2 = joinerRpcClient.sendMessage(serverAddr2,
                RapidRequest.newBuilder()
                        .setProbeMessage(ProbeMessage.getDefaultInstance()).build()).get();
        assertEquals(NodeStatus.BOOTSTRAPPING, probeResponse2.getProbeResponse().getStatus());
    }


    /**
     * Test to ensure that injecting message drops works.
     */
    @Test
    public void droppedMessage() throws InterruptedException,
            IOException, MembershipView.NodeAlreadyInRingException {
        final int serverPort = 1234;
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        final List<ServerDropInterceptors.FirstN> interceptors = new ArrayList<>();
        interceptors.add(new ServerDropInterceptors.FirstN(100, RapidRequest.ContentCase.PROBEMESSAGE));
        createAndStartMembershipService(serverAddr, interceptors);

        final Endpoint clientAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        final IMessagingClient client = new GrpcClient(clientAddr);
        boolean exceptionCaught = false;
        try {
            client.sendMessageBestEffort(serverAddr, Utils.toRapidRequest(ProbeMessage.getDefaultInstance())).get();
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
        final List<Endpoint> endpointList = new ArrayList<>(N);
        final int serverPort = 1234;
        for (int i = 0; i < N; i++) {
            final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort + i + 1);
            createAndStartMembershipService(serverAddr);
            endpointList.add(serverAddr);
        }
        final Endpoint clientAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        final Settings settings = new Settings();
        final IMessagingClient client = new GrpcClient(clientAddr, resources, settings);
        for (int i = 0; i < 10; i++) {
            final List<ListenableFuture<RapidResponse>> futures =
                    client.bestEffortBroadcast(endpointList,
                            Utils.toRapidRequest(FastRoundPhase2bMessage.getDefaultInstance()));
            for (final ListenableFuture<RapidResponse> future : futures) {
                assertNotNull(future);
                final RapidResponse response = future.get();
                assertNotNull(response);
            }
        }
        client.shutdown();
    }


    /**
     * Tests all GrpcClient request types to an endpoint that does not exist, checking if all calls fail.
     */
    @Test
    public void rpcClientErrorHandling() throws InterruptedException {
        final int basePort = 1234;
        final Endpoint clientAddr = Utils.hostFromParts(LOCALHOST_IP, basePort);
        final Endpoint dst = Utils.hostFromParts(LOCALHOST_IP, 4321);
        final Settings settings = new Settings();
        final SharedResources resources = new SharedResources(clientAddr);
        final IMessagingClient client = new GrpcClient(clientAddr, resources, settings);
        try {
            client.sendMessage(dst, Utils.toRapidRequest(ProbeMessage.getDefaultInstance())).get();
            fail("sendProbeMessage did not throw an exception");
        } catch (final ExecutionException | GrpcClient.ShuttingDownException ignored) {
        }
        client.shutdown();
        resources.shutdown();
    }


    /**
     * Tests all GrpcClient request types to an endpoint that exists, but after shutdown is invoked.
     */
    @Test
    public void rpcClientErrorHandlingAfterShutdown() throws InterruptedException {
        final int basePort = 1234;
        final Endpoint clientAddr = Utils.hostFromParts(LOCALHOST_IP, basePort);
        final Endpoint dst = Utils.hostFromParts(LOCALHOST_IP, 4321);
        final SharedResources resources = new SharedResources(clientAddr);
        final Settings settings = new Settings();
        final IMessagingClient client = new GrpcClient(clientAddr, resources, settings);
        client.shutdown();
        resources.shutdown();
        try {
            client.sendMessage(dst, Utils.toRapidRequest(ProbeMessage.getDefaultInstance())).get();
            fail("sendProbeMessage did not throw an exception");
        } catch (final ExecutionException | GrpcClient.ShuttingDownException ignored) {
        }
    }


    /**
     * Tests our broadcaster to make sure it receives responses from all nodes it sends messages to.
     */
    @Test
    public void nettyDirectTests() throws IOException, ExecutionException, InterruptedException {
        final Endpoint serverAddr = Endpoint.newBuilder().setHostname("127.0.0.1").setPort(1234).build();
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        final IMessagingClient client = new GrpcClient(serverAddr);
        final MembershipService service = new MembershipService(serverAddr, watermarkBuffer, membershipView, resources,
                new Settings(), client, new PingPongFailureDetector.Factory(serverAddr, client));
        final NettyDirectTcpClient tcpClient = new NettyDirectTcpClient(resources);
        final NettyDirectTcpServer tcpServer = new NettyDirectTcpServer("127.0.0.1", 1234, resources);
        tcpServer.start();
        tcpServer.setMembershipService(service);
        final RapidRequest request = Utils.toRapidRequest(ProbeMessage.getDefaultInstance());
        try {
            final RapidResponse response = tcpClient.sendMessageBestEffort(serverAddr, request).get();
            assertNotNull(response);
        } catch (final ExecutionException ignored) {
        }
        tcpClient.shutdown();
        tcpServer.shutdown();
    }


    /**
     * Create a membership service listenting on serverAddr
     */
    private void createAndStartMembershipService(final Endpoint serverAddr)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        final IMessagingClient client = new GrpcClient(serverAddr);
        final MembershipService service = new MembershipService(serverAddr, watermarkBuffer, membershipView, resources,
                new Settings(), client, new PingPongFailureDetector.Factory(serverAddr, client));
        final IMessagingServer rpcServer = new GrpcServer(serverAddr, resources, false);
        rpcServer.setMembershipService(service);
        rpcServer.start();
        rpcServers.add(rpcServer);
        services.add(service);
    }

    /**
     * Create a membership service listenting on serverAddr that uses a list of server interceptors.
     */
    private void createAndStartMembershipService(final Endpoint serverAddr,
                                                             final List<ServerDropInterceptors.FirstN> interceptors)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        final IMessagingClient client = new GrpcClient(serverAddr);
        final IMessagingServer rpcServer = new TestingGrpcServer(serverAddr, interceptors, false);
        final MembershipService service = new MembershipService(serverAddr, watermarkBuffer, membershipView, resources,
                new Settings(), client, new PingPongFailureDetector.Factory(serverAddr, client));
        rpcServer.setMembershipService(service);
        rpcServer.start();
        rpcServers.add(rpcServer);
        services.add(service);
    }

    /**
     * Create a membership service listening on serverAddr, with a supplied membershipView and server interceptors.
     */
    private void createAndStartMembershipService(final Endpoint serverAddr,
                                                      final MembershipView membershipView)
            throws IOException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final IMessagingClient client = new GrpcClient(serverAddr);
        final MembershipService service = new MembershipService(serverAddr, watermarkBuffer, membershipView, resources,
                new Settings(), client, new PingPongFailureDetector.Factory(serverAddr, client));
        final IMessagingServer rpcServer = new GrpcServer(serverAddr, resources, false);
        rpcServer.setMembershipService(service);
        rpcServer.start();
        rpcServers.add(rpcServer);
        services.add(service);
    }

    private JoinResponse sendPreJoinMessage(final IMessagingClient client, final Endpoint serverAddr,
                                            final Endpoint clientAddr, final NodeId identifier)
            throws ExecutionException, InterruptedException {
        final RapidRequest preJoinMessage = Utils.toRapidRequest(PreJoinMessage.newBuilder()
                                                            .setSender(clientAddr)
                                                            .setNodeId(identifier).build());
        return client.sendMessage(serverAddr, preJoinMessage).get().getJoinResponse();
    }

    private static class ResponseCallback implements FutureCallback<RapidResponse> {

        @Override
        public void onSuccess(@Nullable final RapidResponse o) {
        }

        @Override
        public void onFailure(final Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}