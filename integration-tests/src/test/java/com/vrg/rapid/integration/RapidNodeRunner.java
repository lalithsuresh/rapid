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

/**
 * Created by lsuresh on 12/1/17.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * RapidNodeRunner
 * To manage and run rapid processes.
 */
class RapidNodeRunner {
    // Interval to wait after shutdown retry
    private static final Long SHUTDOWN_RETRY_WAIT_MS = 500L;
    // Number of retries to kill node before giving up.
    private static final int SHUTDOWN_RETRIES = 10;
    // Timeout for a shutdown (millis)
    private static final int SHUTDOWN_TIMEOUT_MS = 5000;

    // Get Rapid StandAlone runner jar path.
    private static String RAPID_RUNNER_JAR = System.getProperty("rapidExamplesAllInOneJar");
    private static String RAPID_TEST_FOLDER = System.getProperty("java.io.tmpdir");

    private final String seed;
    private final String listenAddress;
    private final String role;
    private final String clusterName;
    private Process rapidProcess;

    RapidNodeRunner(final String seed, final String listenAddress, final String role, final String clusterName) {
        this.seed = seed;
        this.listenAddress = listenAddress;
        this.role = role;
        this.clusterName = clusterName;
    }

    /**
     * Runs the rapid process with the provided parameters.
     *
     * @return RapidNodeRunner
     * @throws IOException if jar or temp directory is not found.
     */
    RapidNodeRunner runNode() throws IOException {

        final File rapidRunnerJar = new File(RAPID_RUNNER_JAR);
        if (!rapidRunnerJar.exists()) {
            throw new FileNotFoundException("Rapid runner jar not found.");
        }
        final String command = "java" +
                " -server" +
                " -jar " + RAPID_RUNNER_JAR +
                " --listenAddress " + listenAddress +
                " --seedAddress " + seed +
                " --role " + role +
                " --cluster " + clusterName;
        final File outputLogFile = new File(RAPID_TEST_FOLDER + File.separator + UUID.randomUUID().toString());
        outputLogFile.deleteOnExit();
        System.out.println("Output for listenAddress:" +
                listenAddress + " logged : " + outputLogFile.getAbsolutePath());

        final ProcessBuilder builder = new ProcessBuilder();
        builder.command("sh", "-c", command).redirectOutput(outputLogFile);
        rapidProcess = builder.start();
        return this;
    }

    /**
     * Returns a reference to the underlying process
     */
    Process getRapidProcess() {
        return rapidProcess;
    }

    /**
     * Kills the process.
     *
     * @return true if kill successful else false.
     */
    boolean killNode() {
        long retries = SHUTDOWN_RETRIES;

        while (true) {

            try {
                rapidProcess.destroyForcibly().waitFor(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }

            if (retries == 0) {
                return false;
            }

            if (rapidProcess.isAlive()) {
                retries--;
                try {
                    Thread.sleep(SHUTDOWN_RETRY_WAIT_MS);
                } catch (final InterruptedException ignored) {
                }
            } else {
                return true;
            }
        }
    }
}
