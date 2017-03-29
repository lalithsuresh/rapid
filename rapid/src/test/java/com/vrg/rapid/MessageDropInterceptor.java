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

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Drops messages at the server of a gRPC call. Used for testing.
 */
class MessageDropInterceptor implements ServerInterceptor {
    private final double dropProbability;

    MessageDropInterceptor(final double probability) {
        if (probability < 0 || probability > 1.0) {
            throw new IllegalArgumentException("Probability must be between 0 and 1.0");
        }
        this.dropProbability = probability;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                                                             final Metadata metadata,
                                                             final ServerCallHandler<ReqT, RespT> serverCallHandler) {
        if (dropProbability >= ThreadLocalRandom.current().nextDouble(1.0)) {
            return new ServerCall.Listener<ReqT>() {};
        }
        else {
            return serverCallHandler.startCall(serverCall, metadata);
        }
    }
}
