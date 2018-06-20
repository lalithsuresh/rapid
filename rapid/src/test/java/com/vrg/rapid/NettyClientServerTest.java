package com.vrg.rapid;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.messaging.impl.NettyClientServer;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class NettyClientServerTest {

    /**
     * Tests NettyClientServer messaging
     */
    @Test
    public void sendMessageNetty() throws IOException, InterruptedException, ExecutionException {
        Cluster serverInstance = null;
        try {
            final int numClients = 100;
            final Endpoint server = Endpoint.newBuilder().setHostname("127.0.0.1")
                    .setPort(9000).build();
            final SharedResources resources = new SharedResources(server);
            final NettyClientServer serverMessaging = new NettyClientServer(server, resources);
            serverInstance = new Cluster.Builder(server)
                    .setMessagingClientAndServer(serverMessaging, serverMessaging)
                    .start();
            assertNotNull(serverInstance);
            final SharedResources shared = new SharedResources(Endpoint.getDefaultInstance());
            final List<NettyClientServer> ncs = new ArrayList<>();
            for (int i = 0; i < numClients; i++) {
                final Endpoint clientEp = Endpoint.newBuilder()
                        .setHostname("127.0.0.1")
                        .setPort(9002 + i).build();
                ncs.add(new NettyClientServer(clientEp, shared));
            }

            final List<ListenableFuture<RapidResponse>> futures = new ArrayList<>();
            final RapidRequest messageToSend = RapidRequest.newBuilder()
                    .setProbeMessage(ProbeMessage.getDefaultInstance())
                    .build();
            for (final NettyClientServer nc : ncs) {
                futures.add(nc.sendMessage(server, messageToSend));
            }
            final List<RapidResponse> responses = Futures.allAsList(futures).get();
            assertNotNull(responses);
            assertEquals(responses.size(), numClients);
        } finally {
            if (serverInstance != null) {
                serverInstance.shutdown();
            }
        }
    }
}
