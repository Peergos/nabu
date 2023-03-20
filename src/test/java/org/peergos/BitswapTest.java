package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;

import java.nio.charset.*;
import java.util.*;

public class BitswapTest {

    @Test
    public void getBlock() {
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore());
        Host node1 = builder1.build();
        RamBlockstore blockstore2 = new RamBlockstore();
        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore2);
        Host node2 = builder2.build();
        node1.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            byte[] blockData = "G'day from Java bitswap!".getBytes(StandardCharsets.UTF_8);
            Cid hash = blockstore2.put(blockData, Cid.Codec.Raw).join();
            Bitswap bitswap1 = builder1.getBitswap().get();
            node1.getAddressBook().addAddrs(address2.getPeerId(), 0, address2).join();
            byte[] receivedBlock = bitswap1.get(hash, node1, Set.of(address2.getPeerId())).join();
            if (! Arrays.equals(receivedBlock, blockData))
                throw new IllegalStateException("Incorrect block returned!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
