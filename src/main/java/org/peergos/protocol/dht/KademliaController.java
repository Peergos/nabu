package org.peergos.protocol.dht;

import kotlin.*;
import org.peergos.protocol.dht.pb.*;

import java.util.concurrent.*;

public interface KademliaController {

    CompletableFuture<Dht.Message> send(Dht.Message msg);

    void receive(Dht.Message msg);

    CompletableFuture<Unit> close();
}
