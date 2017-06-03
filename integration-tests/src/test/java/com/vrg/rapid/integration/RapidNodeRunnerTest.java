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

import org.junit.Assert;
import org.junit.Test;

/**
 * Example Tests for integration tests.
 */
public class RapidNodeRunnerTest extends AbstractMultiJVMTest {

    @Test
    public void runAndAssertSingleNode() throws Exception {
        RapidNodeRunner r = new RapidNodeRunner("127.0.0.1:1234", "127.0.0.1:1234", "testRole", "Rapid")
                .runNode();
        Assert.assertTrue(r.getRapidProcess().isAlive());
        r.killNode();
        Assert.assertFalse(r.getRapidProcess().isAlive());
    }
}
