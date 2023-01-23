package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.bitswap.*;

import java.nio.charset.*;
import java.util.*;

public class BitswapTest {

    @Test
    public void getBlock() {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        Host node1 = Server.buildHost(10000 + new Random().nextInt(50000), bitswap1, Optional.empty());
        RamBlockstore blockstore2 = new RamBlockstore();
        Host node2 = Server.buildHost(10000 + new Random().nextInt(50000), new Bitswap(new BitswapEngine(blockstore2)), Optional.empty());
        node1.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            byte[] blockData = "G'day from Java bitswap!".getBytes(StandardCharsets.UTF_8);
            Cid hash = blockstore2.put(blockData, Cid.Codec.Raw).join();
            BitswapController bc1 = bitswap1.dial(node1, address2).getController().join();
            byte[] receivedBlock = bitswap1.getEngine().get(hash).join();
            if (! Arrays.equals(receivedBlock, blockData))
                throw new IllegalStateException("Incorrect block returned!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
