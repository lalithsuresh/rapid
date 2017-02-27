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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;


/**
 * A configuration represents a series of commitments to either add or
 * remove an identifier from the history. The identifier in this case
 * is a node.
 *
 * The configuration mandates that identifier additions are always unique.
 *
 * TODO: Not used as of now. Remove.
 */
public class Configuration {
    private final ArrayList<String> configHistory;
    private final Set<UUID> identifiersSeen = new HashSet<>(); // TODO: when to gc?
    private static final String ZERO = "zero"; // All histories start from zero.
    private static final String NONE = ""; // Indicates no common ancestor

    Configuration() {
        this.configHistory = new ArrayList<>();
        this.configHistory.add(Utils.sha1Hex(ZERO));
    }

    Configuration(final ArrayList<String> configHistory,
                         final ArrayList<List<UUID>> opHistory) {
        this.configHistory = Objects.requireNonNull(configHistory);
        this.configHistory.add(Utils.sha1Hex(ZERO));
    }

    static ComparisonResult compare(final Configuration c1, final Configuration c2) {
        return compare(c1, c2.configHistory);
    }

    static ComparisonResult compare(final Configuration c1, final List<String> c2) {
        // c1 either subsumes c2 or vice versa
        final int o1size = c1.configHistory.size();
        final int o2size = c2.size();
        assert o1size > 0;
        assert o2size > 0;
        final String c2Head = c2.get(o2size - 1);

        // First we compare the heads to see if they are compatible
        if (c1.head().equals(c2Head)) {
            return ComparisonResult.EQUAL;
        }

        final String divergingCommit = c1.findDivergingCommit(c2);
        // The next case, is either...

        // ... 1) There is no diverging commit, we need to go back further in history or sync
        if (divergingCommit.equals(NONE)) {
            return ComparisonResult.NO_COMMON_ANCESTOR; // do enums for return code checking
        }

        // ... 2) c2 is one or more configUpdates ahead of c1.head()  (c2 > c1).
        if (divergingCommit.equals(c1.head())) {
            return ComparisonResult.FAST_FORWARD_TO_RIGHT; // FF c1 to c2
        }
        // ... 3) c1 is one or more configUpdates ahead of c2.head()  (c1 > c2).
        if (divergingCommit.equals(c2Head)) {
            return ComparisonResult.FAST_FORWARD_TO_LEFT; // FF c2 to c1
        }
        // ... 4) Merge required, since c1 and c2 have both made some updates since the diverging point.
        // merge!
        return ComparisonResult.MERGE;
    }

    void updateConfiguration(final List<UUID> operations) {
        assert operations.size() > 0;
        identifiersSeen.addAll(operations);
        configHistory.add(Utils.sha1Hex(Utils.sha1Hex(identifiersSeen)));
    }

    private String findDivergingCommit(final List<String> remoteConfigHistory) {
        final Set<String> localConfig = new HashSet<>(configHistory);

        for (int i = remoteConfigHistory.size() - 1; i >= 0; i--) {
            final String configId = remoteConfigHistory.get(i);
            if (localConfig.contains(configId)) {
                return configId;
            }
        }

        return NONE;
    }

    String head() {
        final int size = configHistory.size();
        assert size > 0;
        return configHistory.get(size - 1);
    }

    enum ComparisonResult {
        EQUAL,
        FAST_FORWARD_TO_RIGHT,
        FAST_FORWARD_TO_LEFT,
        NO_COMMON_ANCESTOR,
        MERGE,
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final String config: configHistory) {
            sb.append(config);
            sb.append(",");
        }
        return sb.toString();
    }
}