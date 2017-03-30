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

import com.google.common.base.Stopwatch;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.concurrent.TimeUnit;

/**
 * This pretty prints test results when using surefire.
 */
public class CustomRunListener extends RunListener {
    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";
    private static final String RED = "\u001B[31m";
    private static final Description FAILED = Description.createTestDescription("FAILED", "FAILED");
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    @Override
    public void testRunStarted(final Description description) {
        System.out.println("Runtime diagnostics: " + getRuntimeStats());
    }

    @Override
    public void testFinished(final Description description) throws Exception {
        if (!description.getChildren().contains(FAILED)) {
            final long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            System.out.println(GREEN + "     [PASS] [Time: " + elapsedTime + " ms]" + RESET + " " + description);
        }
    }

    @Override
    public void testFailure(final Failure failure) throws Exception {
        final long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.println(RED + "     [FAILED] [Time: " + elapsedTime + " ms]" + getRuntimeStats()
                + RESET + " " + failure);
        System.out.println(RED + "     " + RESET + failure.getException());
        failure.getDescription().addChild(FAILED);
    }

    @Override
    public void testStarted(final Description description) throws Exception {
        stopwatch.reset();
        stopwatch.start();
    }

    private String getRuntimeStats() {
        final Runtime runtime = Runtime.getRuntime();
        return String.format("[Proc:%d Mem:%s Free:%s]", runtime.availableProcessors(),
                              runtime.totalMemory(), runtime.freeMemory());
    }
}
