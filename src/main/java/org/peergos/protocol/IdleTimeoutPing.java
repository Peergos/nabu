package org.peergos.protocol;

import io.libp2p.core.Stream;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/** Ping, closing incoming ping streams their peer has stopped using. The responder echoes for as long as a stream is
 *  open, so a peer opening a new stream for each ping and never closing them would leave them all open here.
 */
public class IdleTimeoutPing extends PingBinding {

    public IdleTimeoutPing() {
        super(new PingProtocol(32) {
            @NotNull
            @Override
            protected CompletableFuture<PingController> onStartResponder(@NotNull Stream stream) {
                StreamTimeouts.closeWhenIdle(stream);
                return super.onStartResponder(stream);
            }
        });
    }
}
