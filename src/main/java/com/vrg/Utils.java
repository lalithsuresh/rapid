package com.vrg;

import org.apache.commons.codec.digest.DigestUtils;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;

/**
 * Utility methods
 */
final class Utils {
    static String sha1HexStringToString(final String text) {
        return DigestUtils.sha1Hex(text);
    }

    static String sha1HexLongsToString(final Collection<Long> input) {
        final MessageDigest md = DigestUtils.getSha1Digest();
        final Long[] longs = input.toArray(new Long[0]);
        Arrays.sort(longs);
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * longs.length);
        for (final Long l: longs) {
            buffer.putLong(l);
        }
        return DigestUtils.sha1Hex(buffer.array());
    }
}
