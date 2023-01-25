package org.peergos.dht;

import io.libp2p.core.*;
import kotlin.*;
import org.peergos.dht.pb.*;

import java.util.concurrent.*;

public class KademliaConnection implements KademliaController {

    private final Stream conn;

    public KademliaConnection(Stream conn) {
        this.conn = conn;
    }

    @Override
    public void send(Dht.Message msg) {
        conn.writeAndFlush(msg);
    }

    @Override
    public CompletableFuture<Unit> close() {
        return conn.close();
    }
}
