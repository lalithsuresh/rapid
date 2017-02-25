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
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Utility methods
 */
final class Utils {
    private static final UUID[] EMPTY_UUID_ARRAY = new UUID[0];
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

    private Utils() {
    }

    static String sha1Hex(final String text) {
        return DigestUtils.sha1Hex(text);
    }

    static String sha1Hex(final Collection<UUID> input) {
        final UUID[] uuids = input.toArray(EMPTY_UUID_ARRAY);
        Arrays.sort(uuids);
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2 * uuids.length); // a UUID = 2 longs.
        for (final UUID id: uuids) {
            buffer.putLong(id.getMostSignificantBits());
            buffer.putLong(id.getLeastSignificantBits());
        }
        return DigestUtils.sha1Hex(buffer.array());
    }

    static long murmurHex(final HostAndPort address, final long seed) {
        final Hasher hasher = HASH_FUNCTION.newHasher();
        return hasher.putString(address.getHostText(), Charset.defaultCharset())
                     .putInt(address.getPort())
                     .putLong(seed)
                     .hash().asInt();
    }

    static long murmurHex(final Collection<UUID> input) {
        final Hasher hasher = HASH_FUNCTION.newHasher();

        for (final UUID id: input) {
            hasher.putLong(id.getMostSignificantBits())
                  .putLong(id.getLeastSignificantBits());
        }

        return  hasher.hash().asInt();
    }

    static class Pair<T1, T2> {
        final T1 first;
        final T2 second;

        Pair(final T1 first, final T2 second) {
            this.first = first;
            this.second = second;
        }
    }
}