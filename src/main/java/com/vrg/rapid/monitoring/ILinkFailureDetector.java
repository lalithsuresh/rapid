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
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.ProbeResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;

/**
 * The LinkFailureDetector interface. Objects that implement this interface can be
 * supplied to the MembershipService to perform failure detection.
 */
public interface ILinkFailureDetector {
    /**
     * Executed at the monitor. Implementors are expected to create a self containing
     * ProbeMessage that will be sent to each monitor.
     */
    ProbeMessage createProbe(final HostAndPort monitoree);

    /**
     * Executed at the monitor on successfully receiving a ProbeResponse for a sent
     * ProbeMessage.
     */
    void handleProbeOnSuccess(final ProbeResponse probeResponse,
                              final HostAndPort monitoree);
    /**
     * Executed at the monitor if an attempt to send a probe threw an exception.
     */
    void handleProbeOnFailure(final Throwable throwable,
                              final HostAndPort monitoree);

    /**
     * Executed at the monitoree upon successfully receiving a probe message from a monitor.
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
