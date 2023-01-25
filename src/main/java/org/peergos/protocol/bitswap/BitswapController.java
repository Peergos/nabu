package org.peergos.protocol.bitswap;

import kotlin.*;
import org.peergos.protocol.bitswap.pb.*;

import java.util.concurrent.*;

public interface BitswapController {

    void send(MessageOuterClass.Message msg);

    CompletableFuture<Unit> close();
}
