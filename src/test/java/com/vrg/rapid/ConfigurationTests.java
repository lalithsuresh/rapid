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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;


/**
 * Configuration comparison tests.
 */
public class ConfigurationTests {

    // If two configs apply the same changes, they will have an equality relation
    @Test
    public void configTestEquals() {
        final Configuration conf1 = new Configuration();
        final Configuration conf2 = new Configuration();

        final long[] longs = new Random().longs().distinct().limit(6).toArray();
        final List<Long> conf1List = new ArrayList<>();
        final List<Long> conf2List = new ArrayList<>();
        for (final long l : longs) {
            conf1List.add(l);
            conf2List.add(l);
        }
        conf1.updateConfiguration(conf1List);
        conf2.updateConfiguration(conf2List);
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf2, conf1));
    }

    // If one config is empty, it always fast-forwards to the other
    @Test
    public void configTestOneEmpty() {
        final Configuration conf1 = new Configuration();
        final Configuration conf2 = new Configuration();

        final long[] longs = new Random().longs().distinct().limit(6).toArray();
        final List<Long> conf1List = new ArrayList<>();
        for (final long l : longs) {
            conf1List.add(l);
        }

        conf1.updateConfiguration(conf1List);
        assertEquals(Configuration.ComparisonResult.FAST_FORWARD_TO_LEFT, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.FAST_FORWARD_TO_RIGHT, Configuration.compare(conf2, conf1));
    }

    // If both configs are empty, they are equal
    @Test
    public void configTestBothEmpty() {
        final Configuration conf1 = new Configuration();
        final Configuration conf2 = new Configuration();

        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf2, conf1));
    }

    // If two configs diverged, despite one being the subset of the other,
    // it calls for a merge
    @Test
    public void configTestMerge() {
        final Configuration conf1 = new Configuration();
        final Configuration conf2 = new Configuration();

        final long[] commits1 = new Random().longs().distinct().limit(6).toArray();
        final List<Long> commitList1 = new ArrayList<>();
        final List<Long> commitList2 = new ArrayList<>();
        for (final long commit : commits1) {
            commitList1.add(commit);
            commitList2.add(commit);
        }

        // Conf1 makes progress from here
        final long[] commits2 = new Random().longs().distinct().limit(6).toArray();
        for (final long commit : commits2) {
            final List<Long> tmp = new ArrayList<>();
            tmp.add(commit);
            conf1.updateConfiguration(tmp); // conf1 will have increments of single commits
        }

        conf1.updateConfiguration(commitList1);
        conf2.updateConfiguration(commitList2);
        assertEquals(Configuration.ComparisonResult.MERGE, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.MERGE, Configuration.compare(conf2, conf1));
    }

    @Test
    public void configTestDivergeAfterZero() {
        final Configuration conf1 = new Configuration();
        final Configuration conf2 = new Configuration();

        final long[] commits1 = new Random().longs().distinct().limit(6).toArray();
        final List<Long> commitList1 = new ArrayList<>();
        final List<Long> commitList2 = new ArrayList<>();
        for (final long commit : commits1) {
            commitList1.add(commit);
            commitList2.add(commit);
        }
        conf1.updateConfiguration(commitList1);
        conf2.updateConfiguration(commitList2);
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf2, conf1));

        // Conf1 makes progress from here
        final long[] commits2 = new Random().longs().distinct().limit(6).toArray();
        for (final long commit : commits2) {
            final List<Long> tmp = new ArrayList<>();
            tmp.add(commit);
            conf1.updateConfiguration(tmp); // conf1 will have increments of single commits
        }

        assertEquals(Configuration.ComparisonResult.FAST_FORWARD_TO_LEFT, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.FAST_FORWARD_TO_RIGHT, Configuration.compare(conf2, conf1));
    }

    // Test for divergence past the 'zero' config state.
    @Test
    public void configTestFFAfterZero() {
        final Configuration conf1 = new Configuration();
        final Configuration conf2 = new Configuration();

        final long[] commits1 = new Random().longs().distinct().limit(6).toArray();
        final List<Long> commitList1 = new ArrayList<>();
        final List<Long> commitList2 = new ArrayList<>();
        for (final long commit : commits1) {
            commitList1.add(commit);
            commitList2.add(commit);
        }
        conf1.updateConfiguration(commitList1);
        conf2.updateConfiguration(commitList2);
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf2, conf1));

        // Conf1 makes progress from here
        final long[] commits2 = new Random().longs().distinct().limit(6).toArray();
        for (final long commit : commits2) {
            final List<Long> singleList = new ArrayList<>();
            singleList.add(commit);
            conf1.updateConfiguration(singleList); // conf1 will have increments of single commits
        }

        commitList2.clear();
        commitList2.add(commits2[0]); // only add the first commit from above to the list
        conf2.updateConfiguration(commitList2);

        assertEquals(Configuration.ComparisonResult.FAST_FORWARD_TO_LEFT, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.FAST_FORWARD_TO_RIGHT, Configuration.compare(conf2, conf1));

        commitList2.clear();
        commitList2.add(commits2[3]); // break the commit order from what conf1 applied. This should trigger a merge
        conf2.updateConfiguration(commitList2);

        assertEquals(Configuration.ComparisonResult.MERGE, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.MERGE, Configuration.compare(conf2, conf1));
    }

    @Test
    public void configTestDifferentStepsAfterZero() {
        final Configuration conf1 = new Configuration();
        final Configuration conf2 = new Configuration();

        final long[] commits1 = new Random().longs().distinct().limit(6).toArray();
        final List<Long> commitList1 = new ArrayList<>();
        final List<Long> commitList2 = new ArrayList<>();
        for (final long commit : commits1) {
            commitList1.add(commit);
            commitList2.add(commit);
        }
        conf1.updateConfiguration(commitList1);
        conf2.updateConfiguration(commitList2);
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf2, conf1));

        commitList2.clear();

        // Diverge from here. conf1 makes 6 individual view changes whereas
        // conf2 applies one view change with 6 operations.
        final long[] commits2 = new Random().longs().distinct().limit(6).toArray();
        for (final long commit : commits2) {
            commitList2.add(commit);

            final List<Long> singleList = new ArrayList<>();
            singleList.add(commit);
            conf1.updateConfiguration(singleList); // conf1 will have increments of single commits
        }
        conf2.updateConfiguration(commitList2);

        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf1, conf2));
        assertEquals(Configuration.ComparisonResult.EQUAL, Configuration.compare(conf2, conf1));
    }
}