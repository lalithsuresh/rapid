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

package com.vrg.rapid.integration;

import io.netty.util.internal.ConcurrentSet;
import org.junit.After;
import org.junit.Before;

import java.util.Set;

/**
 * AbstractMultiJVMTest for running rapid agents.
 */
public class AbstractMultiJVMTest {

    private Set<RapidNodeRunner> rapidNodeRunners;

    @Before
    public void prepare() {
        rapidNodeRunners = new ConcurrentSet<>();
    }

    /**
     * Deletes temp output directory.
     * Cleans up all rapidNodeRunners.
     */
    @After
    public void cleanUp() {
        rapidNodeRunners.forEach(RapidNodeRunner::killNode);
    }

    /**
     * Create and return a new RapidNodeRunner instance.
     */
    RapidNodeRunner createRapidInstance(final String seed, final String listenAddress,
                                        final String role, final String clusterName) {
        final RapidNodeRunner runner = new RapidNodeRunner(seed, listenAddress, role, clusterName);
        rapidNodeRunners.add(runner);
        return runner;
    }
}
