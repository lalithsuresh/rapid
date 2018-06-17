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

/**
 * Event types to subscribe from the cluster.
 */
public enum ClusterEvents {        // Triggered when...
    VIEW_CHANGE_PROPOSAL,          // <- When a node announces a proposal using the multi node cut detector.
    VIEW_CHANGE,                   // <- When a fast-paxos quorum of identical proposals were received.
    VIEW_CHANGE_ONE_STEP_FAILED,   // <- When a fast-paxos quorum of identical proposals is unavailable.
    KICKED,                        // <- When a node detects that it has been removed from the network.
}
