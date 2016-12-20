package com.vrg;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Created by lsuresh on 12/13/16.
 */
class Utils {
    static String sha1Hex(final String text) {
        return DigestUtils.sha1Hex(text);
    }
}
