package org.peergos.protocol.autonat;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tracks AutoNAT v2 dial-back nonces we are expecting. When we (as a dial-request client) ask a server
 * to dial one of our addresses, we send a random nonce; the server proves it reached us by opening a
 * dial-back stream on the new connection and echoing that nonce. This correlates the two streams.
 */
public class NonceRegistry {

    private final Map<Long, CompletableFuture<Boolean>> pending = new ConcurrentHashMap<>();

    /** Register a nonce we are about to send, returning a future that completes when a matching dial-back arrives. */
    public CompletableFuture<Boolean> expect(long nonce) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        pending.put(nonce, future);
        return future;
    }

    /** Called when a dial-back arrives; returns true iff we were expecting this nonce. */
    public boolean fulfil(long nonce) {
        CompletableFuture<Boolean> future = pending.remove(nonce);
        if (future != null) {
            future.complete(true);
            return true;
        }
        return false;
    }

    /** Stop expecting a nonce (e.g. the request failed), resolving its future as not-received. */
    public void forget(long nonce) {
        CompletableFuture<Boolean> future = pending.remove(nonce);
        if (future != null)
            future.complete(false);
    }
}
