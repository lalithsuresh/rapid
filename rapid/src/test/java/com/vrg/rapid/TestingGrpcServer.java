package com.vrg.rapid;

import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.messaging.impl.GrpcServer;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;
import io.grpc.stub.StreamObserver;

import java.util.List;

/**
 * A wrapper around GrpcServer that is used for testing.
 */
public class TestingGrpcServer extends GrpcServer {
    private final List<ServerDropInterceptors.FirstN> interceptors;

    TestingGrpcServer(final Endpoint address, final List<ServerDropInterceptors.FirstN> interceptors,
                      final boolean useInProcessTransport) {
        super(address, new SharedResources(address), useInProcessTransport);
        this.interceptors = interceptors;
    }

    /**
     * Defined in rapid.proto.
     */
    @Override
    public void sendRequest(final RapidRequest rapidRequest,
                            final StreamObserver<RapidResponse> responseObserver) {
        for (final ServerDropInterceptors.FirstN interceptor: interceptors) {
            if (!interceptor.filter(rapidRequest)) {
                return;
            }
        }
        super.sendRequest(rapidRequest, responseObserver);
    }
}
