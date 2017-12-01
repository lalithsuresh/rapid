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

import com.vrg.rapid.pb.RapidRequest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drops messages at the server of a gRPC call. Used for testing.
 */
class ServerDropInterceptors {
    /**
     * Drops the first N messages of a particular type.
     */
    static class FirstN {
        private final AtomicInteger counter;
        private final RapidRequest.ContentCase requestCase;

        FirstN(final int N, final RapidRequest.ContentCase requestCase) {
            if (N < 1) {
                throw new IllegalArgumentException("N must be >= 1");
            }
            this.counter = new AtomicInteger(N);
            this.requestCase = requestCase;
        }

        public boolean filter(final RapidRequest request) {
            return !(request.getContentCase().equals(requestCase)
                    && counter.getAndDecrement() >= 0);
        }
    }
}

/**
 * Drops messages at the client of a gRPC call. Used for testing.
 */
class ClientInterceptors {
    static class Delayer {
        private final CountDownLatch latch;
        private final RapidRequest.ContentCase requestCase;

        /**
         * Drops messages of a specific type until the latch permits.
         */
        Delayer(final CountDownLatch latch, final RapidRequest.ContentCase requestCase) {
            this.latch = latch;
            this.requestCase = requestCase;
        }

        boolean filter(final RapidRequest request) {
            if (request.getContentCase().equals(requestCase)) {
                try {
                    latch.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return true;
        }
    }
}