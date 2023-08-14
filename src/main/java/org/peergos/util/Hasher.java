package org.peergos.util;

import org.peergos.Hash;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.concurrent.CompletableFuture;

public class Hasher {

    public CompletableFuture<byte[]> sha256(byte[] input) {
        return CompletableFuture.completedFuture(Hash.sha256(input));
    }

    public CompletableFuture<byte[]> hmacSha256(byte[] secretKeyBytes, byte[] message) {
        try {
            String algorithm = "HMACSHA256";
            Mac mac = Mac.getInstance(algorithm);
            SecretKey secretKey = new SecretKeySpec(secretKeyBytes, algorithm);
            mac.init(secretKey);
            return Futures.of(mac.doFinal(message));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
