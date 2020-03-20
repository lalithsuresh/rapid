package com.vrg.rapid;

import com.vrg.rapid.messaging.impl.GrpcClient;

/**
 * Holds configuration parameters for different components of a Rapid instance.
 */
public final class Settings implements GrpcClient.ISettings, MembershipService.ISettings {
    private boolean useInProcessTransport = GrpcClient.DEFAULT_GRPC_USE_IN_PROCESS_TRANSPORT;
    private int grpcTimeoutMs = GrpcClient.DEFAULT_GRPC_TIMEOUT_MS;
    private int grpcDefaultRetries = GrpcClient.DEFAULT_GRPC_DEFAULT_RETRIES;
    private int grpcJoinTimeoutMs = GrpcClient.DEFAULT_GRPC_JOIN_TIMEOUT;
    private int grpcProbeTimeoutMs = GrpcClient.DEFAULT_GRPC_PROBE_TIMEOUT;
    private int failureDetectorIntervalInMs = MembershipService.DEFAULT_FAILURE_DETECTOR_INTERVAL_IN_MS;
    private int batchingWindowInMs = MembershipService.BATCHING_WINDOW_IN_MS;

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
}
