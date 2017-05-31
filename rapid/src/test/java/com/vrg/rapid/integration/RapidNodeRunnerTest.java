package com.vrg.rapid.integration;

import org.junit.Assert;
import org.junit.Test;

/**
 * Example Tests for integration tests.
 * Created by zlokhandwala on 5/31/17.
 */
public class RapidNodeRunnerTest extends AbstractIT {

    @Test
    public void runAndAssertSingleNode() throws Exception {
        RapidNodeRunner r = new RapidNodeRunner("127.0.0.1:1234", "127.0.0.1:1234", "testRole", "Rapid")
                .killOnExit()
                .runNode();
        Assert.assertTrue(r.getRapidProcess().isAlive());
    }
}
