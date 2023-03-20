package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;

public class KademliaTest {

    @Test
    @Ignore // until auto relay is done
    public void findRelayedNode() throws Exception {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore1, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();

        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node2 = builder2.build();
        node2.start().join();

        try {
            // bootstrap node 2
            Kademlia dht2 = builder2.getWanDht().get();
            dht2.bootstrapRoutingTable(node2, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht2.bootstrap(node2);

            // bootstrap node 1
            Kademlia dht1 = builder1.getWanDht().get();
            dht1.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht1.bootstrap(node1);

            // Check node1 can find node2 from kubo
            Multihash peerId2 = Multihash.deserialize(node2.getPeerId().getBytes());
            List<PeerAddresses> closestPeers = dht1.findClosestPeers(peerId2, 2, node1);
            Optional<PeerAddresses> matching = closestPeers.stream()
                    .filter(p -> p.peerId.equals(peerId2))
                    .findFirst();
            if (matching.isEmpty())
                throw new IllegalStateException("Couldn't find node2 from kubo!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
