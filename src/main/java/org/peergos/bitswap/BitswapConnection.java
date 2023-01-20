package org.peergos.bitswap;

import bitswap.message.pb.*;
import io.libp2p.core.Stream;
import kotlin.*;

import java.util.concurrent.*;

public class BitswapConnection implements BitswapController {

    private final Stream conn;

    public BitswapConnection(Stream conn) {
        this.conn = conn;
    }

    @Override
    public void send(MessageOuterClass.Message msg) {
        conn.writeAndFlush(msg);
    }

    @Override
    public CompletableFuture<Unit> close() {
        return conn.close();
    }
}
