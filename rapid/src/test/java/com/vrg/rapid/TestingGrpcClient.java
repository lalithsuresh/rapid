package com.vrg.rapid;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.vrg.rapid.messaging.impl.GrpcClient;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

import java.util.List;

/**
 * GrpcClient with interceptors used for testing
 */
public class TestingGrpcClient extends GrpcClient {
    final List<ClientInterceptors.Delayer> interceptors;

    public TestingGrpcClient(final HostAndPort address, final ISettings settings,
                             final List<ClientInterceptors.Delayer> interceptors) {
        super(address, settings);
        this.interceptors = interceptors;
    }

    /**
     * From IMessagingClient
     */
    @Override
    public ListenableFuture<RapidResponse> sendMessage(final HostAndPort remote, final RapidRequest msg) {
        for (final ClientInterceptors.Delayer interceptor: interceptors) {
            if (!interceptor.filter(msg)) {
                return Futures.immediateFuture(RapidResponse.getDefaultInstance());
            }
        }
        return super.sendMessage(remote, msg);
    }
}
