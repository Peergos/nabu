package org.peergos.dht;

import kotlin.*;
import org.peergos.dht.pb.*;

import java.util.concurrent.*;

public interface KademliaController {

    void send(Dht msg);

    CompletableFuture<Unit> close();
}
