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

import com.google.protobuf.ByteString;
import com.vrg.rapid.messaging.IBroadcaster;
import com.vrg.rapid.messaging.IMessagingClient;
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.monitoring.impl.PingPongFailureDetector;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.FastRoundPhase2bMessage;
import com.vrg.rapid.pb.Proposal;
import com.vrg.rapid.pb.RapidRequest;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the MembershipService class without the messaging.
 */
@RunWith(JUnitParamsRunner.class)
public class FastPaxosWithoutFallbackTests {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;
    private final List<MembershipService> services = new ArrayList<>();

    @After
    public void cleanup() {
        for (final MembershipService service: services) {
            service.shutdown();
        }
        services.clear();
    }

    /**
     * Verifies that a node makes a decision only after |quorum| identical proposals are received.
     * This test does not generate conflicting proposals.
     */
    @Test
    @Parameters(method = "fastQuorumTestNoConflictsData")
    @TestCaseName("{method}[N={0},Q={1}]")
    public void fastQuorumTestNoConflicts(final int N, final int quorum) throws InterruptedException,
            ExecutionException {
        final int serverPort = 1234;
        final Endpoint node = Utils.hostFromParts("127.0.0.1", serverPort);
        final Endpoint proposalNode = Utils.hostFromParts("127.0.0.1", serverPort + 1);
        final MembershipView view = createView(serverPort, N);
        final MembershipService service = createAndStartMembershipService(node, view);
        assertEquals(N, service.getMembershipSize());
        final long currentId = view.getCurrentConfigurationId();
        for (int i = 0; i < quorum - 1; i++) {
            final FastRoundPhase2bMessage.Builder proposal =
                    getProposal(addrForBase(serverPort + i), currentId, view.getRing(0),
                            Collections.singletonList(proposalNode));
            service.handleMessage(asRapidMessage(proposal.build())).get();
            assertEquals(N, service.getMembershipSize());
        }
        final FastRoundPhase2bMessage.Builder proposal =
                getProposal(addrForBase(serverPort + quorum - 1), currentId, view.getRing(0),
                        Collections.singletonList(proposalNode));
        service.handleMessage(asRapidMessage(proposal.build())).get();
        assertEquals(N - 1, service.getMembershipSize());
    }

    public static Iterable<Object[]> fastQuorumTestNoConflictsData() {
        return Arrays.asList(new Object[][] {
                {6, 5}, {48, 37}, {50, 38}, {100, 76}, {102, 77}, // Even N
                {5, 4}, {51, 39}, {49, 37}, {99, 75}, {101, 76}   // Odd N
        });
    }


    /**
     * Verifies that a node makes a decision only after |quorum| identical proposals are received.
     * This test generates conflicting proposals.
     */
    @Test
    @Parameters(method = "fastQuorumTestWithConflicts")
    @TestCaseName("{method}[N={0},Q={1},Conflicts={2},ShouldChange={3}]")
    public void fastQuorumTestWithConflicts(final int N, final int quorum, final int numConflicts,
                                            final boolean changeExpected)
            throws InterruptedException, IOException, ExecutionException {
        final int serverPort = 1234;
        final Endpoint node = Utils.hostFromParts("127.0.0.1", serverPort);
        final Endpoint proposalNode = Utils.hostFromParts("127.0.0.1", serverPort + 1);
        final Endpoint proposalNodeConflict = Utils.hostFromParts("127.0.0.1", serverPort + 2);
        final MembershipView view = createView(serverPort, N);
        final MembershipService service = createAndStartMembershipService(node, view);
        assertEquals(N, service.getMembershipSize());
        final long currentId = view.getCurrentConfigurationId();

        for (int i = 0; i < numConflicts; i++) {
            final FastRoundPhase2bMessage.Builder proposalConflict =
                    getProposal(addrForBase(serverPort + i), currentId, view.getRing(0),
                            Collections.singletonList(proposalNodeConflict));
            service.handleMessage(asRapidMessage(proposalConflict.build())).get();
            assertEquals(N, service.getMembershipSize());
        }
        final int nonConflictCount = Math.min(numConflicts + quorum - 1, N - 1);
        for (int i = numConflicts; i < nonConflictCount; i++) {
            final FastRoundPhase2bMessage.Builder proposal =
                    getProposal(addrForBase(serverPort + i), currentId, view.getRing(0),
                            Collections.singletonList(proposalNode));
            service.handleMessage(asRapidMessage(proposal.build())).get();
            assertEquals(N, service.getMembershipSize());
        }
        final FastRoundPhase2bMessage.Builder proposal =
                getProposal(addrForBase(serverPort + nonConflictCount), currentId, view.getRing(0),
                        Collections.singletonList(proposalNode));

        service.handleMessage(asRapidMessage(proposal.build())).get();
        assertEquals(changeExpected ? N - 1 : N, service.getMembershipSize());
    }

    public static Iterable<Object[]> fastQuorumTestWithConflicts() {
        // Format: (N, quorum size, number of conflicts, should-membership-change)
        // One conflicting message. Must lead to decision.
        final List<Object[]> oneConflictArray = Arrays.asList(new Object[][] {
                {6, 5, 1, true}, {48, 37, 1, true},  {50, 38, 1, true},  {100, 76, 1, true},  {102, 77, 1, true},
        });
        // Boundary case: F conflicts, and N-F non-conflicts. Must lead to decisions.
        final List<Object[]> boundaryCase = Arrays.asList(new Object[][] {
                                 {48, 37, 11, true}, {50, 38, 12, true}, {100, 76, 24, true}, {102, 77, 25, true},
        });
        // More conflicts than Fast Paxos quorum size. These must not lead to decisions.
        final List<Object[]> tooManyConflicts = Arrays.asList(new Object[][] {
                {6, 5, 2, false}, {48, 37, 14, false}, {50, 38, 13, false}, {100, 76, 25, false}, {102, 77, 26, false},
        });
        final ArrayList<Object[]> params = new ArrayList<>();
        params.addAll(oneConflictArray);
        params.addAll(boundaryCase);
        params.addAll(tooManyConflicts);
        return params;
    }

    /**
     * Create a membership service listening on serverAddr
     */
    private MembershipService createAndStartMembershipService(final Endpoint serverAddr, final MembershipView view)
            throws MembershipView.NodeAlreadyInRingException {
        final MultiNodeCutDetector cutDetector =
                new MultiNodeCutDetector(K, H, L);
        final SharedResources resources = new SharedResources(serverAddr);
        final IMessagingClient client = new GrpcClient(serverAddr);
        final IBroadcaster broadcaster = new UnicastToAllBroadcaster(client);
        final MembershipService service = new MembershipService(serverAddr, cutDetector, view,
                resources, new Settings(), client, broadcaster,
                new PingPongFailureDetector.Factory(serverAddr, client));
        services.add(service);
        return service;
    }

    /**
     * Populates a view with a sequence of Hosts starting from basePort up to basePort + N - 1
     */
    private MembershipView createView(final int basePort, final int N) {
        final MembershipView view = new MembershipView(K);
        for (int i = basePort; i < basePort + N; i++) {
            view.ringAdd(Utils.hostFromParts("127.0.0.1", i), Utils.nodeIdFromUUID(UUID.randomUUID()));
        }
        return view;
    }

    /**
     * Returns a proposal message without the sender set.
     */
    private FastRoundPhase2bMessage.Builder getProposal(final Endpoint node,
                                                        final long currentConfigurationId,
                                                        final List<Endpoint> memberList,
                                                        final List<Endpoint> proposal) {
        final BitSet votes = new BitSet(memberList.size());
        votes.set(memberList.indexOf(node));
        return FastRoundPhase2bMessage.newBuilder()
                .addProposals(Proposal.newBuilder()
                    .addAllEndpoints(proposal)
                    .setConfigurationId(currentConfigurationId)
                    .setVotes(ByteString.copyFrom(votes.toByteArray()))
                    .build());
    }

    private Endpoint addrForBase(final int port) {
        return Utils.hostFromParts("127.0.0.1", port);
    }

    private RapidRequest asRapidMessage(final FastRoundPhase2bMessage proposal) {
        return Utils.toRapidRequest(proposal);
    }
}
