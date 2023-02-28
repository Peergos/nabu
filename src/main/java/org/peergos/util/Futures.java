package org.peergos.util;

import java.util.*;

public class Futures {

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
