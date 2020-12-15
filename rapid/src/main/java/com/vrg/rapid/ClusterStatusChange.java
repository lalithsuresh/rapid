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

package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;

import java.util.List;

public class ClusterStatusChange {
    private final long configurationId;
    private final List<Endpoint> membership;
    private final List<NodeStatusChange> delta;

    public ClusterStatusChange(final long configurationId, final List<Endpoint> membership,
                               final List<NodeStatusChange> delta) {
        this.configurationId = configurationId;
        this.membership = membership;
        this.delta = delta;
    }

    /**
     * @return the configuration ID represented by the this change event
     */
    public long getConfigurationId() {
        return configurationId;
    }

    /**
     * @return the membership in the configuration corresponding to getConfigurationId()
     */
    public List<Endpoint> getMembership() {
        return membership;
    }

    /**
     * @return the delta corresponding to this event
     */
    public List<NodeStatusChange> getDelta() {
        return delta;
    }

    @Override
    public String toString() {
        return "ClusterStatusChange{" +
                "configurationId=" + configurationId +
                ", membership=" + membership +
                ", delta=" + delta +
                '}';
    }
}