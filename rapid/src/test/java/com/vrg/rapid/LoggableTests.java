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
