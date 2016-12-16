package com.vrg;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Created by lsuresh on 12/13/16.
 */
public class Utils {
    public static String sha1Hex(String text) {
        return DigestUtils.sha1Hex(text.toString());
    }
}
