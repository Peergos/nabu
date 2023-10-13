package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;

public class KademliaTest {

    @Test
    public void findOtherNode() throws Exception {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(), new RamRecordStore(), blockstore1, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1);

        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node2 = builder2.build();
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2);

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

    @Ignore
    @Test
    public void ipnsBenchmark() throws Exception {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(), new RamRecordStore(), blockstore1, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1);

        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true));
        Host node2 = builder2.build();
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2);

        Cid value = blockstore1.put("Provide me.".getBytes(), Cid.Codec.Raw).join();

        try {
            // bootstrap node 1
            Kademlia dht1 = builder1.getWanDht().get();
            dht1.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht1.bootstrap(node1);

            // bootstrap node 2
            Kademlia dht2 = builder2.getWanDht().get();
            dht2.bootstrapRoutingTable(node2, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht2.bootstrap(node2);

            List<PrivKey> signers = new ArrayList<>();
            for (int i=0; i < 10; i++) {
                // publish mapping from node 1
                PrivKey signer = Ed25519Kt.generateEd25519KeyPair().getFirst();
                signers.add(signer);
                Multihash pub = Multihash.deserialize(PeerId.fromPubKey(signer.publicKey()).getBytes());
                long p0 = System.currentTimeMillis();
                dht1.publishIpnsValue(signer, pub, value, 1, node1).join();
                long p1 = System.currentTimeMillis();
                System.out.println("Publish took " + (p1-p0) + "ms");

                // retrieve it from node 2
                long t0 = System.currentTimeMillis();
                String res = dht2.resolveIpnsValue(pub, node2, 1).orTimeout(10, TimeUnit.SECONDS).join();
                long t1 = System.currentTimeMillis();
                Assert.assertTrue(res.equals("/ipfs/" + value));
                System.out.println("Resolved in " + (t1 - t0) + "ms");
            }

            // retrieve all again
            for (PrivKey signer : signers) {
                Multihash pub = Multihash.deserialize(PeerId.fromPubKey(signer.publicKey()).getBytes());
                long t0 = System.currentTimeMillis();
                String res = dht2.resolveIpnsValue(pub, node2, 1).orTimeout(10, TimeUnit.SECONDS).join();
                long t1 = System.currentTimeMillis();
                Assert.assertTrue(res.equals("/ipfs/" + value));
                System.out.println("Resolved again in " + (t1 - t0) + "ms");
            }
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
