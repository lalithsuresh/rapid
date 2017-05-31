package com.vrg.rapid.integration;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.lang.reflect.Field;
import java.nio.file.Paths;

import static java.nio.file.Files.write;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;

import com.google.common.io.Files;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Assert;

/**
 * AbstractIT for running rapid agents.
 * <p>
 * Created by zlokhandwala on 5/30/17.
 */
public class AbstractIT {

    private static String RAPID_PROJECT_DIR = new File("..").getAbsolutePath() + File.separator;
    private static String RAPID_RUNNER_JAR = RAPID_PROJECT_DIR + "rapid-examples/target/rapid-examples-1.0-SNAPSHOT-allinone.jar";
    private static String KILL_COMMAND = "kill -9 ";

    // Interval to wait after shutdown retry
    private static Long SHUTDOWN_RETRY_WAIT = 500L;
    // Number of retries to kill node before giving up.
    private static int SHUTDOWN_RETRIES = 10;

    private static Set<RapidNodeRunner> rapidNodeRunners;

    public AbstractIT() {
        rapidNodeRunners = new HashSet<>();
    }

    @Getter
    private static File OUTPUT_LOG_DIR;

    @Before
    public void setUp() {
        OUTPUT_LOG_DIR = Files.createTempDir();
    }

    /**
     * Deletes temp output directory.
     * Cleans up all deleteOnExit rapidNodeRunners.
     */
    @After
    public void cleanUp() {
        OUTPUT_LOG_DIR.deleteOnExit();
        // remove if kill successful
        rapidNodeRunners.removeIf(rapidNodeRunner -> {
            if (rapidNodeRunner.killOnExit) {
                if (!rapidNodeRunner.isKilled) {
                    try {
                        return rapidNodeRunner.killNode();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    return true;
                }
            }
            return false;
        });
    }

    /**
     * Cleanup all runners.
     */
    @AfterClass
    public static void cleanUpEnv() {
        rapidNodeRunners.forEach(rapidNodeRunner -> {
            if (!rapidNodeRunner.isKilled) {
                try {
                    rapidNodeRunner.killNode();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * RapidNodeRunner
     * To manage and run rapid processes.
     */
    @Accessors(chain = true)
    class RapidNodeRunner {

        private final String seed;
        private final String listenAddress;
        private final String role;
        private final String clusterName;

        @Getter
        private Process rapidProcess;

        private boolean isKilled = true;
        private boolean killOnExit = false;

        RapidNodeRunner(String seed, String listenAddress, String role, String clusterName) {
            this.seed = seed;
            this.listenAddress = listenAddress;
            this.role = role;
            this.clusterName = clusterName;
        }

        private class OutputLogger implements Runnable {
            private InputStream inputStream;
            private String logfile;

            private OutputLogger(InputStream inputStream, String logfile) {
                this.inputStream = inputStream;
                this.logfile = logfile;
            }

            @Override
            public void run() {
                new BufferedReader(new InputStreamReader(inputStream)).lines()
                        .forEach((x) -> {
                                    try {
                                        write(Paths.get(logfile), x.getBytes());
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                        );
            }
        }

        /**
         * Runs the rapid process with the provided parameters.
         *
         * @return RapidNodeRunner
         * @throws IOException if jar or temp directory is not found.
         */
        RapidNodeRunner runNode() throws IOException {

            File rapidRunnerJar = new File(RAPID_RUNNER_JAR);
            if (!rapidRunnerJar.exists()) {
                throw new FileNotFoundException("Rapid runner jar not found.");
            }
            String command = "java" +
                    " -server" +
                    " -jar " + RAPID_RUNNER_JAR +
                    " --listenAddress " + this.listenAddress +
                    " --seedAddress " + this.seed +
                    " --role " + this.role +
                    " --cluster " + this.clusterName;

            File outputLogFile = new File(OUTPUT_LOG_DIR + File.separator + UUID.randomUUID().toString());
            System.out.println("Output for listenAddress:" + listenAddress + " logged : " + outputLogFile.getAbsolutePath());

            ProcessBuilder builder = new ProcessBuilder();
            builder.command("sh", "-c", command);
            builder.directory(new File(RAPID_PROJECT_DIR));
            Process rapidProcess = builder.start();
            OutputLogger outputLogger = new OutputLogger(rapidProcess.getInputStream(), outputLogFile.getAbsolutePath());
            Executors.newSingleThreadExecutor().submit(outputLogger);
            this.rapidProcess = rapidProcess;
            isKilled = false;
            rapidNodeRunners.add(this);

            return this;
        }

        private long getPid(Process p) {
            long pid = -1;
            try {
                if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                    Field f = p.getClass().getDeclaredField("pid");
                    f.setAccessible(true);
                    pid = f.getLong(p);
                    f.setAccessible(false);
                }
            } catch (Exception e) {
                pid = -1;
            }
            return pid;
        }

        /**
         * Kills the process on test exit.
         *
         * @return
         */
        RapidNodeRunner killOnExit() {
            killOnExit = true;
            return this;
        }

        /**
         * Kills the process.
         *
         * @return true if kill successful else false.
         * @throws IOException          if I/O error occurs.
         * @throws InterruptedException if kill interrupted.
         */
        boolean killNode() throws IOException, InterruptedException {
            Assert.assertNotNull(rapidProcess);
            long retries = SHUTDOWN_RETRIES;

            while (true) {
                Long pid = getPid(rapidProcess);
                ProcessBuilder builder = new ProcessBuilder();
                builder.command("sh", "-c", KILL_COMMAND + pid);
                Process p = builder.start();
                p.waitFor();

                if (retries == 0) {
                    return false;
                }

                if (rapidProcess.isAlive()) {
                    retries--;
                    Thread.sleep(SHUTDOWN_RETRY_WAIT);
                } else {
                    isKilled = true;
                    return true;
                }
            }
        }
    }
}
