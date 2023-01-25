package org.peergos.bitswap;

import kotlin.*;
import org.peergos.bitswap.pb.*;

import java.util.concurrent.*;

public interface BitswapController {

    void send(MessageOuterClass.Message msg);

    CompletableFuture<Unit> close();
}
