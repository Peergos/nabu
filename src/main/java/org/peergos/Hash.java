package org.peergos;

import java.security.*;

public class Hash {

    public static byte[] sha256(byte[] in) {
        try {
            MessageDigest hasher = MessageDigest.getInstance("SHA-256");
            return hasher.digest(in);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
