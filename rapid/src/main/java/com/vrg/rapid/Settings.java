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

import com.vrg.rapid.messaging.impl.GrpcClient;

/**
 * Holds configuration parameters for different components of a Rapid instance.
 */
public final class Settings implements GrpcClient.ISettings, MembershipService.ISettings, FastPaxos.ISettings {
    private boolean useInProcessTransport = GrpcClient.DEFAULT_GRPC_USE_IN_PROCESS_TRANSPORT;
    private int grpcTimeoutMs = GrpcClient.DEFAULT_GRPC_TIMEOUT_MS;
    private int grpcDefaultRetries = GrpcClient.DEFAULT_GRPC_DEFAULT_RETRIES;
    private int grpcJoinTimeoutMs = GrpcClient.DEFAULT_GRPC_JOIN_TIMEOUT;
    private int grpcProbeTimeoutMs = GrpcClient.DEFAULT_GRPC_PROBE_TIMEOUT;
    private int failureDetectorIntervalInMs = MembershipService.DEFAULT_FAILURE_DETECTOR_INTERVAL_IN_MS;
    private int batchingWindowInMs = MembershipService.BATCHING_WINDOW_IN_MS;
    private long consensusFallbackTimeoutBaseDelayInMs = FastPaxos.BASE_DELAY;
    private int membershipViewTimeoutInMs = MembershipService.MEMBERSHIP_VIEW_TIMEOUT_IN_MS;

    /*
     * Settings from GrpcClient.ISettings
     */
    @Override
    public boolean getUseInProcessTransport() {
        return useInProcessTransport;
    }

    public void setUseInProcessTransport(final boolean useInProcessTransport) {
        this.useInProcessTransport = useInProcessTransport;
    }

    @Override
    public int getGrpcTimeoutMs() {
        return grpcTimeoutMs;
    }

    public void setGrpcTimeoutMs(final int grpcTimeoutMs) {
        this.grpcTimeoutMs = grpcTimeoutMs;
    }

    @Override
    public int getGrpcDefaultRetries() {
        return grpcDefaultRetries;
    }

    public void setGrpcDefaultRetries(final int grpcDefaultRetries) {
        this.grpcDefaultRetries = grpcDefaultRetries;
    }

    @Override
    public int getGrpcJoinTimeoutMs() {
        return grpcJoinTimeoutMs;
    }

    public void setGrpcJoinTimeoutMs(final int grpcJoinPhaseTwoTimeoutMs) {
        this.grpcJoinTimeoutMs = grpcJoinPhaseTwoTimeoutMs;
    }

    @Override
    public int getGrpcProbeTimeoutMs() {
        return grpcProbeTimeoutMs;
    }

    public void setGrpcProbeTimeoutMs(final int grpcProbeTimeoutMs) {
        this.grpcProbeTimeoutMs = grpcProbeTimeoutMs;
    }


    /*
     * Settings from MembershipService.ISettings
     */
    @Override
    public int getFailureDetectorIntervalInMs() {
        return failureDetectorIntervalInMs;
    }

    public void setFailureDetectorIntervalInMs(final int failureDetectorIntervalInMs) {
        this.failureDetectorIntervalInMs = failureDetectorIntervalInMs;
    }

    @Override
    public int getBatchingWindowInMs() {
        return this.batchingWindowInMs;
    }

    public void setBatchingWindowInMs(final int batchingWindowInMs) {
        this.batchingWindowInMs = batchingWindowInMs;
    }


    @Override
    public int getMembershipViewUpdateTimeoutInMs() {
        return this.membershipViewTimeoutInMs;
    }

    public void setMembershipViewUpdateTimeoutInMs(final int membershipViewTimeoutInMs) {
        this.membershipViewTimeoutInMs = membershipViewTimeoutInMs;
    }

    /*
     * Settings from FastPaxos.ISettings
     */
    @Override
    public long getConsensusFallbackTimeoutBaseDelayInMs() {
        return this.consensusFallbackTimeoutBaseDelayInMs;
    }

    public void setConsensusFallbackTimeoutBaseDelayInMs(final long consensusFallbackTimeoutBaseDelayInMs) {
            this.consensusFallbackTimeoutBaseDelayInMs = consensusFallbackTimeoutBaseDelayInMs;
    }
}
