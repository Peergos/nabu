package org.peergos.util;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Futures {

    public static final <T> CompletableFuture<T> of(T val) {
        return CompletableFuture.completedFuture(val);
    }

    public static <T> T logAndThrow(Throwable t) {
        return logAndThrow(t, Optional.empty());
    }

    private static <T> T logAndThrow(Throwable t, Optional<String> message) {
        if (message.isPresent())
            System.out.println(message);
        t.printStackTrace();
        throw new RuntimeException(t.getMessage(), t);
    }
}
