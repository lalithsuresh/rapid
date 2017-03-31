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

import com.google.common.net.HostAndPort;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drops messages at the server of a gRPC call. Used for testing.
 */
class ServerDropInterceptors {
    /**
     * Drops messages with a fixed probability
     */
    static class FixedProbability implements ServerInterceptor {
        private final double dropProbability;

        FixedProbability(final double probability) {
            if (probability < 0 || probability > 1.0) {
                throw new IllegalArgumentException("Probability must be between 0 and 1.0");
            }
            this.dropProbability = probability;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                                     final Metadata metadata, final ServerCallHandler<ReqT, RespT> serverCallHandler) {
            if (dropProbability >= ThreadLocalRandom.current().nextDouble(1.0)) {
                return new ServerCall.Listener<ReqT>() {};
            }
            else {
                return serverCallHandler.startCall(serverCall, metadata);
            }
        }
    }

    /**
     * Drops the first N messages of a particular type.
     */
    static class FirstN<T1, T2> implements ServerInterceptor {
        private final AtomicInteger counter;
        private final MethodDescriptor<T1, T2> methodDescriptor;
        private final HostAndPort host;

        FirstN(final int N, final MethodDescriptor<T1, T2> methodDescriptor, final HostAndPort host) {
            if (N < 1) {
                throw new IllegalArgumentException("N must be >= 1");
            }
            this.counter = new AtomicInteger(N);
            this.methodDescriptor = methodDescriptor;
            this.host = host;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                                     final Metadata metadata, final ServerCallHandler<ReqT, RespT> serverCallHandler) {
            if (methodDescriptor.getFullMethodName().equals(serverCall.getMethodDescriptor().getFullMethodName())
                    && counter.getAndDecrement() >= 0) {
                return new ServerCall.Listener<ReqT>() {};
            }
            return serverCallHandler.startCall(serverCall, metadata);
        }
    }
}

/**
 * Drops messages at the client of a gRPC call. Used for testing.
 */
class ClientInterceptors {
    static class Delayer<T1, T2> implements ClientInterceptor {
        private final CountDownLatch latch;
        private final MethodDescriptor<T1, T2> methodToblock;

        /**
         * Drops messages of a specific type until the latch permits.
         */
        Delayer(final CountDownLatch latch, final MethodDescriptor<T1, T2> methodToblock) {
            this.latch = latch;
            this.methodToblock = methodToblock;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                               final CallOptions callOptions, final Channel channel) {
            if (methodToblock.getFullMethodName().equals(methodDescriptor.getFullMethodName())) {
                try {
                    latch.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return channel.newCall(methodDescriptor, callOptions);
        }
    }
}