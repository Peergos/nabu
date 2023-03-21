package org.peergos;

import io.ipfs.multihash.*;
import io.libp2p.core.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;

public class FindPeerTest {

    @Test
    public void findLongRunningNode() {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore1, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();

        try {
            // bootstrap node 1
            Kademlia dht1 = builder1.getWanDht().get();
            dht1.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht1.bootstrap(node1);

            // Check node1 can find a long running node
            Multihash toFind = Multihash.fromBase58("QmVdFZgHnEgcedCS2G2ZNiEN59LuVrnRm7z3yXtEBv2XiF");
            findPeer(toFind, dht1, node1);
            findPeer(toFind, dht1, node1);
        } finally {
            node1.stop();
        }
    }

    private static void findPeer(Multihash toFind, Kademlia dht1, Host node1) {
        long t1 = System.currentTimeMillis();
        List<PeerAddresses> closestPeers = dht1.findClosestPeers(toFind, 1, node1);
        long t2 = System.currentTimeMillis();
        Optional<PeerAddresses> matching = closestPeers.stream()
                .filter(p -> p.peerId.equals(toFind))
                .findFirst();
        if (matching.isEmpty())
            throw new IllegalStateException("Couldn't find node2 from kubo!");
        System.out.println("Peer lookup took " + (t2-t1) + "ms");
    }
}
