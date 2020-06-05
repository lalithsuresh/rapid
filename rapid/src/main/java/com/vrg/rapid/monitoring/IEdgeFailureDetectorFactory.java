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

package com.vrg.rapid.monitoring;

import com.vrg.rapid.pb.Endpoint;
import io.grpc.ExperimentalApi;

/**
 * The EdgeFailureDetector interface. Implementations of this interface can be
 * supplied to the MembershipService to perform failure detection.
 **
 * On every configuration change, createInstance() is invoked by the membership
 * service at a node to instantiate new monitoring edges to its subjects.
 *
 * createInstance() must return a runnable that is expected to be periodically
 * executed.
 *
 * To mark an edge faulty, simply execute notifier.run().
 */
@ExperimentalApi
public interface IEdgeFailureDetectorFactory {
    Runnable createInstance(final Endpoint subject, final long configuraitonId, final Runnable notifier);
}