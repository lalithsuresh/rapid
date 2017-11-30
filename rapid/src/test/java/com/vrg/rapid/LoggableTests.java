package com.vrg.rapid;

import com.google.common.collect.ImmutableList;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.Phase1bMessage;
import com.vrg.rapid.pb.Rank;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertTrue;

/**
 * Tests for loggable wrapper around protobuf objects
 */
public class LoggableTests {
    @Test
    public void testToString() throws IOException, InterruptedException {
        final String logString = ("" + Utils.loggable(getMessage()));
        assertTrue(!logString.contains("\n"));
    }

    @Test
    public void testCollectionToString() throws IOException, InterruptedException {
        final List<Phase1bMessage> messages = ImmutableList.of(getMessage(), getMessage());
        final String logString = ("" + Utils.loggable(messages));
        assertTrue(!logString.contains("\n"));
    }

    private Phase1bMessage getMessage() {
        final Rank rank = Rank.newBuilder().setRound(1).setNodeIndex(2).build();
        final List<Endpoint> endpoints =
                ImmutableList.of(Endpoint.newBuilder().setHostname("127.0.0.1").setPort(50).build(),
                        Endpoint.newBuilder().setHostname("127.0.0.2").setPort(51).build());
        return Phase1bMessage.newBuilder()
                .setRnd(rank)
                .addAllVval(endpoints)
                .build();
    }
}
