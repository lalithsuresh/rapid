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

package com.vrg.rapid.monitoring;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.ExperimentalApi;
import io.grpc.stub.StreamObserver;

import java.util.List;

/**
 * The LinkFailureDetector interface. Objects that implement this interface can be
 * supplied to the MembershipService to perform failure detection.
 */
@ExperimentalApi
public interface ILinkFailureDetector {
    /**
     * Executed at the monitor. Implementors are expected to create a self containing
     * ProbeMessage that will be sent to each monitor.
     */
    ListenableFuture<Void> checkMonitoree(final HostAndPort monitoree);

    /**
     * Executed at the monitoree upon successfully receiving a probe message from a monitor.
     *
     * TODO: Expected to be removed when failure detectors can register their own gRPC services.
     */
    void handleProbeMessage(final ProbeMessage probeMessage,
                            final StreamObserver<ProbeResponse> probeResponseStreamObserver);

    /**
     * True if the link has failed. False otherwise. Once an implementation returns false for a monitor,
     * it cannot renege on saying so.
     */
    boolean hasFailed(final HostAndPort monitorees);

    /**
     * Callback when the set of monitorees for a node changes.
     */
    void onMembershipChange(final List<HostAndPort> monitorees);
}
