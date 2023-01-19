package org.peergos.bitswap;

import bitswap.message.pb.*;
import kotlin.*;

import java.util.concurrent.*;

public interface BitswapController {

    void send(MessageOuterClass.Message msg);

    CompletableFuture<Unit> close();
}
