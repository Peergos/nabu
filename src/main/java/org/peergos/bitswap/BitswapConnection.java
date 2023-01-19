package org.peergos.bitswap;

import bitswap.message.pb.*;
import io.ipfs.cid.*;
import io.libp2p.core.Stream;
import kotlin.*;

import java.util.concurrent.*;
import java.util.stream.*;

public class BitswapConnection implements BitswapController {
    public final LinkedBlockingDeque<MessageOuterClass.Message> incoming = new LinkedBlockingDeque<>(10);

    private final Stream conn;

    public BitswapConnection(Stream conn) {
        this.conn = conn;
    }

    @Override
    public void send(MessageOuterClass.Message msg) {
        conn.writeAndFlush(msg);
    }

    public void receive(MessageOuterClass.Message msg) {
        incoming.add(msg);
        System.out.println("Received message: " + msg.getWantlist().getEntriesList().stream()
                .map(e -> Cid.cast(e.getBlock().toByteArray()))
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Unit> close() {
        return conn.close();
    }
}
