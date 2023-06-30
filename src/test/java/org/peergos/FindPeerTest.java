package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;

public class FindPeerTest {

    @Test
    public void findLongRunningNode() {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
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
            Assert.assertTrue(findAndDialPeer(toFind, dht1, node1) < 1_000);
            Assert.assertTrue(findAndDialPeer(toFind, dht1, node1) < 50);
        } finally {
            node1.stop();
        }
    }

    private static long findAndDialPeer(Multihash toFind, Kademlia dht1, Host node1) {
        long t1 = System.currentTimeMillis();
        List<PeerAddresses> closestPeers = dht1.findClosestPeers(toFind, 1, node1);
        long t2 = System.currentTimeMillis();
        Optional<PeerAddresses> matching = closestPeers.stream()
                .filter(p -> p.peerId.equals(toFind))
                .findFirst();
        if (matching.isEmpty())
            throw new IllegalStateException("Couldn't find node2 from kubo!");
        PeerAddresses peer = matching.get();
        Multiaddr[] addrs = peer.getPublicAddresses().stream().map(a -> Multiaddr.fromString(a.toString())).toArray(Multiaddr[]::new);
        dht1.dial(node1, PeerId.fromBase58(peer.peerId.toBase58()), addrs)
                .getController().join().closerPeers(toFind).join();
        System.out.println("Peer lookup took " + (t2-t1) + "ms");
        return t2 - t1;
    }
}
