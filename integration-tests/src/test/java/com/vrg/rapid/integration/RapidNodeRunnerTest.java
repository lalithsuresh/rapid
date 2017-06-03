package com.vrg.rapid.integration;

import org.junit.Assert;
import org.junit.Test;

/**
 * Example Tests for integration tests.
 */
public class RapidNodeRunnerTest extends AbstractMultiJVMTest {

    @Test
    public void runAndAssertSingleNode() throws Exception {
        final RapidNodeRunner rapidNodeRunner =
                new RapidNodeRunner("127.0.0.1:1234", "127.0.0.1:1234", "testRole", "Rapid")
                .runNode();
        Assert.assertTrue(rapidNodeRunner.getRapidProcess().isAlive());
        rapidNodeRunner.killNode();
        Assert.assertFalse(rapidNodeRunner.getRapidProcess().isAlive());
    }
}
