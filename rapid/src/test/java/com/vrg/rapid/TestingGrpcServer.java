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
