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

package com.vrg.rapid.messaging;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.RapidRequest;
import com.vrg.rapid.pb.RapidResponse;

/**
 * Represents the sending part of the messaging API
 */
public interface IMessagingClient {
    /**
     * Send a message to a remote node with re-transmissions if necessary
     *
     * @param remote Remote host to send the message to
     * @param msg Message to send
     * @return A future that returns a RapidResponse if the call was successful.
     */
    @CanIgnoreReturnValue
    ListenableFuture<RapidResponse> sendMessage(final Endpoint remote, final RapidRequest msg);

    /**
     * Send a message to a remote node with best-effort guarantees
     *
     * @param remote Remote host to send the message to
     * @param msg Message to send
     * @return A future that returns a RapidResponse if the call was successful.
     */
    @CanIgnoreReturnValue
    ListenableFuture<RapidResponse> sendMessageBestEffort(final Endpoint remote, final RapidRequest msg);

    /**
     * Signals to the messaging client that it should cleanup all resources in use.
     */
    void shutdown();
}