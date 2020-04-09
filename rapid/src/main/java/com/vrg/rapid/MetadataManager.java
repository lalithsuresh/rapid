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
import com.vrg.rapid.pb.Metadata;
import io.grpc.ExperimentalApi;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages per-node metadata which is immutable. These are simple tags like roles or other configuration parameters.
 */
@ExperimentalApi
@NotThreadSafe
final class MetadataManager {
    private final Map<Endpoint, Metadata> roleMap = new ConcurrentHashMap<>();

    /**
     * Get the list of roles for a node.
     *
     * @param node Node for which the roles are being set.
     */
    Metadata get(final Endpoint node) {
        Objects.requireNonNull(node);
        return roleMap.containsKey(node) ? roleMap.get(node) : Metadata.getDefaultInstance();
    }

    /**
     * Specify a set of roles for a node.
     *
     * @param roles The list of roles for the node.
     */
    void addMetadata(final Map<Endpoint, Metadata> roles) {
        Objects.requireNonNull(roles);
        roles.forEach(roleMap::putIfAbsent);
    }

    /**
     * Remove a node from the role-set. Will throw NPE if called on a node that is not in the set.
     *
     * @param node Node for which the roles are being set.
     */
    void removeNode(final Endpoint node) {
        Objects.requireNonNull(node);
        roleMap.remove(node);
    }

    /**
     * Get the list of all node tags. This is shared with joining nodes when they bootstrap.
     */
    Map<Endpoint, Metadata> getAllMetadata() {
        return roleMap;
    }
}
