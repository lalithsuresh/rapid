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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.net.HostAndPort;

import java.nio.charset.Charset;

/**
 * Utility methods
 */
final class Utils {
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

    private Utils() {
    }

    static long murmurHex(final HostAndPort address, final long seed) {
        final Hasher hasher = HASH_FUNCTION.newHasher();
        return hasher.putString(address.getHost(), Charset.defaultCharset())
                     .putInt(address.getPort())
                     .putLong(seed)
                     .hash().asInt();
    }
}