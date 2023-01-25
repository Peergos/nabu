package org.peergos.protocol.dht;

import kotlin.*;
import org.peergos.protocol.dht.pb.*;

import java.util.concurrent.*;

public interface KademliaController {

    void send(Dht.Message msg);

    CompletableFuture<Unit> close();
}
