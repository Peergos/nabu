package org.peergos.protocol.bitswap;

import io.libp2p.core.multistream.*;

public class Bitswap extends StrictProtocolBinding<BitswapController> {
    public static int MAX_MESSAGE_SIZE = 2*1024*1024;

    private final BitswapEngine engine;

    public Bitswap(BitswapEngine engine) {
        super("/ipfs/bitswap/1.2.0", new BitswapProtocol(engine));
        this.engine = engine;
    }

    public BitswapEngine getEngine() {
        return engine;
    }
}
