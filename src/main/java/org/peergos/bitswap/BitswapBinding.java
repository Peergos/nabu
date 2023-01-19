package org.peergos.bitswap;

import io.libp2p.core.*;
import io.libp2p.core.multistream.*;
import org.jetbrains.annotations.*;

public class BitswapBinding extends StrictProtocolBinding<BitswapController> {
    public BitswapBinding(@NotNull P2PChannelHandler<BitswapController> protocol) {
        super("/ipfs/bitswap/1.2.0", protocol);
    }
}
