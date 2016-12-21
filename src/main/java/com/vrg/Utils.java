package com.vrg;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Utility methods
 */
final class Utils {
    static String sha1Hex(final String text) {
        return DigestUtils.sha1Hex(text);
    }
}
