package org.peergos.protocol;

import io.libp2p.core.Stream;
import io.netty.handler.timeout.*;

/** Bounds on how long a stream can stay open waiting for its peer. A host keeps every open stream until it closes,
 *  so a peer that opens streams and never finishes with them would otherwise grow that set without limit.
 *  Mutable so tests can shorten them.
 */
public final class StreamTimeouts {
    /** An incoming stream on which the peer sends nothing for this long is closed. */
    public static volatile int INCOMING_IDLE_SECONDS = 60;
    /** A p2p http request with no response after this long fails, and its stream is closed. */
    public static volatile int HTTP_RESPONSE_SECONDS = 300;

    private StreamTimeouts() {}

    /** Must be pushed before the protocol's own handler, so it sees every read. */
    public static void closeWhenIdle(Stream stream) {
        stream.pushHandler(new ReadTimeoutHandler(INCOMING_IDLE_SECONDS));
    }
}
