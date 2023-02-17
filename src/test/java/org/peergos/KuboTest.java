package org.peergos;

import io.ipfs.api.*;
import io.ipfs.cid.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.blockstore.*;

import java.io.*;
import java.nio.charset.*;
import java.util.*;

public class KuboTest {

    @Test
    public void getBlock() throws IOException {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        Host node1 = HostBuilder.build(10000 + new Random().nextInt(50000), List.of(bitswap1));
        node1.start().join();
        try {
            IPFS kubo = new IPFS("localhost", 5001);
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kubo.id().get("ID"));
            byte[] blockData = "G'day from Java bitswap!".getBytes(StandardCharsets.UTF_8);
            Cid hash = (Cid)kubo.block.put(List.of(blockData), Optional.of("raw")).get(0).hash;
            BitswapController bc1 = bitswap1.dial(node1, address2).getController().join();
            byte[] receivedBlock = bitswap1.get(hash, node1).join();
            if (! Arrays.equals(receivedBlock, blockData))
                throw new IllegalStateException("Incorrect block returned!");
        } finally {
            node1.stop();
        }
    }
}
