package org.peergos.bitswap;

public class Bitswap extends BitswapBinding {
    public static int MAX_MESSAGE_SIZE = 2*1024*1024;

    private final BitswapEngine engine;

    public Bitswap(BitswapEngine engine) {
        super(new BitswapProtocol(engine));
        this.engine = engine;
    }

    public BitswapEngine getEngine() {
        return engine;
    }
}
