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

package com.vrg.rapid.integration;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Example Tests for integration tests.
 */
public class RapidNodeRunnerTest extends AbstractMultiJVMTest {
    @Test
    public void runAndAssertSingleNode() throws Exception {
        final RapidNodeRunner rapidNodeRunner =
                createRapidInstance("127.0.0.1:1234", "127.0.0.1:1234", "testRole", "Rapid")
                        .runNode();
        assertTrue(rapidNodeRunner.getRapidProcess().isAlive());
        rapidNodeRunner.killNode();
        assertFalse(rapidNodeRunner.getRapidProcess().isAlive());
    }

    @Test
    public void runAndAssertMultipleNodes() throws Exception {
        final int numNodes = 60;
        final RapidNodeRunner seed =
                createRapidInstance("127.0.0.1:1234", "127.0.0.1:1234",
                                    "testRole", "Rapid")
                        .runNode();
        final List<RapidNodeRunner> nodes = IntStream.range(0, numNodes - 1)
                .mapToObj(i -> createRapidInstance("127.0.0.1:1234", "127.0.0.1:" + (1235 + i),
                                                   "testRole", "Rapid"))
                .collect(Collectors.toList());
        nodes.forEach(n -> {
            try {
                n.runNode();
            } catch (final IOException e) {
                e.printStackTrace();
            }
        });
        nodes.add(seed);
        Thread.sleep(10000);
        for (final RapidNodeRunner runner: nodes) {
            final int search = runner.searchFile("Cluster size " + numNodes);
            assertTrue("Instances: " + search + " " + runner,search > 0);
        }
    }
}