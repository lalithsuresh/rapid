package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.vrg.rapid.pb.ConsensusProposal;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for the MembershipService class without the messaging.
 */
@RunWith(JUnitParamsRunner.class)
public class MembershipServiceTest {
    private static final int K = 10;
    private static final int H = 8;
    private static final int L = 3;
    private final List<MembershipService> services = new ArrayList<>();

    @After
    public void cleanup() throws InterruptedException {
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
    public void fastQuorumTestNoConflicts(final int N, final int quorum) throws InterruptedException, IOException,
            MembershipView.NodeAlreadyInRingException, ExecutionException {
        final int serverPort = 1234;
        final HostAndPort node = HostAndPort.fromParts("127.0.0.1", serverPort);
        final HostAndPort proposalNode = HostAndPort.fromParts("127.0.0.1", serverPort + 1);
        final MembershipView view = createView(serverPort, N);
        final MembershipService service = createAndStartMembershipService(node, view);
        assertEquals(N, service.getMembershipSize());
        final long currentId = view.getCurrentConfigurationId();
        final ConsensusProposal.Builder proposal =
                getProposal(currentId, Collections.singletonList(proposalNode));

        for (int i = 0; i < quorum - 1; i++) {
            service.processConsensusProposal(proposal.setSender(addrForBase(i).toString()).build());
            assertEquals(N, service.getMembershipSize());
        }
        service.processConsensusProposal(proposal.setSender(addrForBase(quorum - 1).toString()).build());
        assertEquals(N - 1, service.getMembershipSize());
    }

    public static Iterable<Object[]> fastQuorumTestNoConflictsData() {
        return Arrays.asList(new Object[][] {
                {6, 5}, {48, 37}, {50, 38}, {100, 76}, {102, 77}, // Even N
                {5, 4}, {51, 39}, {49, 37}, {99, 75}, {101, 76}   // Odd N
        });
    }

    /**
     * Create a membership service listening on serverAddr
     */
    private MembershipService createAndStartMembershipService(final HostAndPort serverAddr, final MembershipView view)
            throws IOException, MembershipView.NodeAlreadyInRingException {
        final WatermarkBuffer watermarkBuffer = new WatermarkBuffer(K, H, L);
        final SharedResources resources = new SharedResources(serverAddr);
        final MembershipService service =
                new MembershipService.Builder(serverAddr, watermarkBuffer, view, resources)
                        .build();
        services.add(service);
        return service;
    }

    /**
     * Populates a view with a sequence of HostAndPorts starting from basePort up to basePort + N - 1
     */
    private MembershipView createView(final int basePort, final int N) {
        final MembershipView view = new MembershipView(K);
        for (int i = basePort; i < basePort + N; i++) {
            view.ringAdd(HostAndPort.fromParts("127.0.0.1", i), UUID.randomUUID());
        }
        return view;
    }

    /**
     * Returns a proposal message without the sender set.
     */
    private ConsensusProposal.Builder getProposal(final long currentConfigurationId, final List<HostAndPort> proposal) {
        return ConsensusProposal.newBuilder()
                .setConfigurationId(currentConfigurationId)
                .addAllHosts(proposal
                        .stream()
                        .map(HostAndPort::toString)
                        .sorted()
                        .collect(Collectors.toList()));
    }

    private HostAndPort addrForBase(final int port) {
        return HostAndPort.fromParts("127.0.0.1", port);
    }

}
