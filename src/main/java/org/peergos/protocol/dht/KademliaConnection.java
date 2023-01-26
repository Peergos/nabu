package org.peergos.protocol.dht;

import io.libp2p.core.*;
import kotlin.*;
import org.peergos.protocol.dht.pb.*;

import java.util.concurrent.*;

public class KademliaConnection implements KademliaController {

    private final Stream conn;
    private final LinkedBlockingDeque<CompletableFuture<Dht.Message>> queue = new LinkedBlockingDeque<>();

    public KademliaConnection(Stream conn) {
        this.conn = conn;
    }

    @Override
    public CompletableFuture<Dht.Message> rpc(Dht.Message msg) {
        conn.writeAndFlush(msg);
        CompletableFuture<Dht.Message> res = new CompletableFuture<>();
        queue.add(res);
        return res;
    }

    @Override
    public CompletableFuture<Boolean> send(Dht.Message msg) {
        conn.writeAndFlush(msg);
        return CompletableFuture.completedFuture(true);
    }

    public void receive(Dht.Message msg) {
        queue.poll().complete(msg);
    }

    @Override
    public CompletableFuture<Unit> close() {
        return conn.close();
    }
}
