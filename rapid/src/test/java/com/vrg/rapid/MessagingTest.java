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
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.messaging.IMessagingServer;
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.messaging.impl.GrpcServer;
import com.vrg.rapid.monitoring.impl.PingPongFailureDetector;
import com.vrg.rapid.pb.AlertMessage;
import com.vrg.rapid.pb.BatchedAlertMessage;
import com.vrg.rapid.pb.EdgeStatus;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.JoinResponse;
import com.vrg.rapid.pb.JoinStatusCode;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.NodeStatus;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    private static final String LOCALHOST = "localhost";
    private static final String LOCALHOST_IP = "127.0.0.1";
    private final List<IMessagingServer> rpcServers = new ArrayList<>();
    private final List<MembershipService> services = new ArrayList<>();
    @Nullable private SharedResources resources = null;

    @Before
    public void prepare() {
        resources = new SharedResources(Utils.hostFromParts(LOCALHOST_IP, SERVER_PORT_BASE));
    }

    @After
    public void cleanup() {
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
        final JoinResponse result = sendJoinMessage(client, serverAddr, clientAddr,
                                                       Utils.nodeIdFromUUID(UUID.randomUUID()));
        assertNotNull(result);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, result.getStatusCode());
        assertEquals(2, result.getEndpointsCount());
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
        final JoinResponse result1 = sendJoinMessage(client1, serverAddr, clientAddr1,
                                                             Utils.nodeIdFromUUID(UUID.randomUUID()));
        assertNotNull(result1);
        assertEquals(JoinStatusCode.HOSTNAME_ALREADY_IN_RING, result1.getStatusCode());
        assertEquals(0, result1.getEndpointsCount());
        assertEquals(0, result1.getIdentifiersCount());

        // Try again with a different port, this should fail because we're using the same
        // uuid as the server.
        final int clientPort2 = 1235;
        // we need another address than 127.0.0.1 or else we'll get a valid response from the SAME_NODE_ALREADY_IN_RING
        // mechanism
        final Endpoint clientAddr2 = Utils.hostFromParts(LOCALHOST, clientPort2);
        final GrpcClient client2 = new GrpcClient(clientAddr2);
        final JoinResponse result2 = sendJoinMessage(client2, serverAddr, clientAddr2, nodeIdentifier);
        assertNotNull(result2);
        assertEquals(JoinStatusCode.UUID_ALREADY_IN_RING, result2.getStatusCode());
        assertEquals(0, result2.getEndpointsCount());
        assertEquals(0, result2.getIdentifiersCount());
    }

    /**
     * A node joins while the seed node is undergoing a view change. It should be told to retry
     */
    @Test
    public void joinDuringViewChange() throws InterruptedException, IOException, ExecutionException {
        final int serverPort = 1234;
        final int client1Port = 1235;
        final int client2Port = 1236;
        final int otherNodePort = 1240;

        final NodeId nodeIdentifier = Utils.nodeIdFromUUID(UUID.randomUUID());
        final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        final TestMembershipView membershipView = new TestMembershipView(K);
        membershipView.ringAdd(serverAddr, nodeIdentifier);
        createAndStartMembershipService(serverAddr, membershipView);

        final Endpoint otherEndpoint = Utils.hostFromParts(LOCALHOST_IP, otherNodePort);
        final Endpoint client1Endpoint = Utils.hostFromParts(LOCALHOST_IP, client1Port);

        final ExecutorService executor = Executors.newWorkStealingPool(2);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch testLatch = new CountDownLatch(2);

        membershipView.toggleWait();

        try {
            executor.execute(() -> {
                try {
                    // simulate alert that leads to view change
                    final BatchedAlertMessage.Builder batchedAlertMessage = BatchedAlertMessage
                            .newBuilder()
                            .setSender(otherEndpoint);

                    final List<Endpoint> observers = membershipView.getExpectedObserversOf(client1Endpoint);
                    for (int i = 0; i < observers.size(); i++) {
                        final AlertMessage msg = AlertMessage.newBuilder()
                                .setEdgeSrc(observers.get(i))
                                .setEdgeDst(client1Endpoint)
                                .setEdgeStatus(EdgeStatus.UP)
                                .setConfigurationId(membershipView.getCurrentConfigurationId())
                                .setNodeId(Utils.nodeIdFromUUID(UUID.randomUUID()))
                                .addRingNumber(i)
                                .build();
                        batchedAlertMessage.addMessages(msg);
                    }

                    startLatch.countDown();
                    final GrpcClient client = new GrpcClient(otherEndpoint);
                    client.sendMessage(serverAddr, Utils.toRapidRequest(batchedAlertMessage.build()));
                } finally {
                    testLatch.countDown();
                }
            });
            executor.execute(() -> {
                try {
                    // client attempts joins
                    final Endpoint clientAddr = Utils.hostFromParts(LOCALHOST_IP, client2Port);
                    final GrpcClient client = new GrpcClient(clientAddr);
                    startLatch.await();
                    final JoinResponse result = sendJoinMessage(client, serverAddr, clientAddr,
                            Utils.nodeIdFromUUID(UUID.randomUUID()));
                    assertNotNull(result);
                    assertEquals(JoinStatusCode.VIEW_CHANGE_IN_PROGRESS, result.getStatusCode());
                    membershipView.toggleWait();
                } catch (final InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    fail();
                } finally {
                    testLatch.countDown();
                }
            });
            testLatch.await();
        } finally {
            executor.shutdown();
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
        final JoinResponse response = sendJoinMessage(messagingClient, serverAddr, joinerAddress, joinerUuid);
        assertNotNull(response);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
        assertEquals(2, response.getEndpointsCount());
        assertEquals(response.getConfigurationId(), membershipView.getCurrentConfigurationId());

        // Verify that the hostnames retrieved at the joining peer
        // matches that of the seed node.
        final List<Endpoint> memberList = response.getEndpointsList();

        final Iterator<Endpoint> iterJoiner = memberList.iterator();
        final Iterator<Endpoint> iterSeed = membershipView.getRing(0).iterator();
        for (int i = 0; i < 2; i++) {
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
        final JoinResponse response = sendJoinMessage(messagingClient, serverAddr, joinerAddress, joinerUuid);
        assertNotNull(response);
        assertEquals(JoinStatusCode.SAFE_TO_JOIN, response.getStatusCode());
        assertEquals(2, response.getEndpointsCount());
        assertEquals(response.getConfigurationId(), membershipView.getCurrentConfigurationId());

        // Verify that the hostnames retrieved at the joining peer
        // matches that of the seed node.
        final List<Endpoint> memberList = response.getEndpointsList();

        final Iterator<Endpoint> iterJoiner = memberList.iterator();
        final Iterator<Endpoint> iterSeed = membershipView.getRing(0).iterator();
        for (int i = 0; i < 2; i++) {
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
        membershipView.ringAdd(serverAddr2, nodeIdentifier2); // This causes server1 to observer server2
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
        final int serverPort = 1234;
        final Endpoint clientAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort);
        final Settings settings = new Settings();
        final IMessagingClient client = new GrpcClient(clientAddr, resources, settings);
        final UnicastToAllBroadcaster broadcaster = new UnicastToAllBroadcaster(client);
        for (int i = 0; i < N; i++) {
            final Endpoint serverAddr = Utils.hostFromParts(LOCALHOST_IP, serverPort + i + 1);
            createAndStartMembershipService(serverAddr);
            broadcaster.onNodeAdded(serverAddr, Optional.empty());
        }
        for (int i = 0; i < 10; i++) {
            final List<ListenableFuture<RapidResponse>> futures =
                    broadcaster.broadcast(Utils.toRapidRequest(FastRoundPhase2bMessage.getDefaultInstance()),
                            1L);
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
        } catch (final UncheckedExecutionException e) {
            if (!(e.getCause() instanceof GrpcClient.ShuttingDownException)) {
                throw e;
            }
        }
    }

    /**
     * Create a membership service listenting on serverAddr
     */
    private void createAndStartMembershipService(final Endpoint serverAddr)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final MultiNodeCutDetector cutDetector =
                new MultiNodeCutDetector(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        final IMessagingClient client = new GrpcClient(serverAddr);
        final IBroadcaster broadcaster = new UnicastToAllBroadcaster(client);
        final MembershipService service = new MembershipService(serverAddr, cutDetector,
            membershipView, resources, new Settings(), client, broadcaster,
            new PingPongFailureDetector.Factory(serverAddr, client));
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
        final MultiNodeCutDetector cutDetector =
                new MultiNodeCutDetector(K, H, L);
        final MembershipView membershipView = new MembershipView(K);
        membershipView.ringAdd(serverAddr, Utils.nodeIdFromUUID(UUID.randomUUID()));
        final IMessagingClient client = new GrpcClient(serverAddr);
        final IBroadcaster broadcaster = new UnicastToAllBroadcaster(client);
        final IMessagingServer rpcServer = new TestingGrpcServer(serverAddr, interceptors, false);
        final MembershipService service = new MembershipService(serverAddr, cutDetector,
                membershipView, resources, new Settings(), client, broadcaster,
                new PingPongFailureDetector.Factory(serverAddr, client));
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
        final MultiNodeCutDetector cutDetector =
                new MultiNodeCutDetector(K, H, L);
        final IMessagingClient client = new GrpcClient(serverAddr);
        final IBroadcaster broadcaster = new UnicastToAllBroadcaster(client);
        final MembershipService service = new MembershipService(serverAddr, cutDetector,
            membershipView, resources, new Settings(), client, broadcaster,
                new PingPongFailureDetector.Factory(serverAddr, client));
        final IMessagingServer rpcServer = new GrpcServer(serverAddr, resources, false);
        rpcServer.setMembershipService(service);
        rpcServer.start();
        rpcServers.add(rpcServer);
        services.add(service);
    }

    private JoinResponse sendJoinMessage(final IMessagingClient client, final Endpoint serverAddr,
                                         final Endpoint clientAddr, final NodeId identifier)
            throws ExecutionException, InterruptedException {
        final RapidRequest joinMessage = Utils.toRapidRequest(JoinMessage.newBuilder()
                                                            .setSender(clientAddr)
                                                            .setNodeId(identifier).build());
        return client.sendMessage(serverAddr, joinMessage).get().getJoinResponse();
    }

}