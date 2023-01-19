package org.peergos.bitswap;

public class Bitswap extends BitswapBinding {
    public static int MAX_MESSAGE_SIZE = 2*1024*1024;

    public Bitswap() {
        super(new BitswapProtocol());
    }
}
