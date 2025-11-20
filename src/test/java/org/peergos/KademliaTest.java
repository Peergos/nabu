package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.concurrent.*;

public class KademliaTest {

    @Ignore
    @Test
    public void findOtherNode() throws Exception {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), blockstore1, (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1, Collections.emptyList());

        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node2 = builder2.build();
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2, Collections.emptyList());

        try {
            // bootstrap node 2
            Kademlia dht2 = builder2.getWanDht().get();
            dht2.bootstrapRoutingTable(node2, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht2.bootstrap(node2);
            new Thread(() -> {
                for (int i=0; i < 20; i++)
                    try {
                        dht2.bootstrap(node2);
                    } catch (Exception e) {}
            }).start();

            // bootstrap node 1
            Kademlia dht1 = builder1.getWanDht().get();
            dht1.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, addr -> !addr.contains("/wss/"));
            dht1.bootstrap(node1);
            new Thread(() -> {
                for (int i=0; i < 20; i++)
                    try {
                        dht1.bootstrap(node1);
                    } catch (Exception e) {}
            }).start();

            // Check node1 can find node2
            Multihash peerId2 = Multihash.deserialize(node2.getPeerId().getBytes());
            List<PeerAddresses> closestPeers = dht1.findClosestPeers(peerId2, 2, node1);
            Optional<PeerAddresses> matching = closestPeers.stream()
                    .filter(p -> p.peerId.equals(peerId2))
                    .findFirst();
            if (matching.isEmpty())
                throw new IllegalStateException("Couldn't find node2!");
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
                new RamProviderStore(1000), new RamRecordStore(), blockstore1, (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1, Collections.emptyList());

        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        Host node2 = builder2.build();
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2, Collections.emptyList());

        Cid value = blockstore1.put("Publish me.".getBytes(), Cid.Codec.Raw).join();

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
            long publishTotal = 0, resolveTotal = 0;
            int iterations = 25;
            for (int i = 0; i < iterations; i++) {
                // publish mapping from node 1
                PrivKey signer = Ed25519Kt.generateEd25519KeyPair().getFirst();
                signers.add(signer);
                Multihash pub = Multihash.deserialize(PeerId.fromPubKey(signer.publicKey()).getBytes());
                long p0 = System.currentTimeMillis();
                int publishes = dht1.publishIpnsValue(signer, pub, value, 1, node1).join();
                long p1 = System.currentTimeMillis();
                System.out.println("Publish took " + printSeconds(p1-p0) + "s to " + publishes + " peers.");
                publishTotal += p1-p0;

                // retrieve it from node 2
                long t0 = System.currentTimeMillis();
                String res = dht2.resolveIpnsValue(pub, node2, 1).orTimeout(10, TimeUnit.SECONDS).join();
                long t1 = System.currentTimeMillis();
                Assert.assertTrue(res.equals("/ipfs/" + value));
                System.out.println("Resolved in " + printSeconds(t1 - t0) + "s");
                resolveTotal += t1-t0;
            }
            System.out.println("Publish av: " + printSeconds(publishTotal/iterations)
                    + ", resolve av: " + printSeconds(resolveTotal/iterations));

            // retrieve all again
            for (PrivKey signer : signers) {
                Multihash pub = Multihash.deserialize(PeerId.fromPubKey(signer.publicKey()).getBytes());
                long t0 = System.currentTimeMillis();
                String res = dht2.resolveIpnsValue(pub, node2, 1).orTimeout(10, TimeUnit.SECONDS).join();
                long t1 = System.currentTimeMillis();
                Assert.assertTrue(res.equals("/ipfs/" + value));
                System.out.println("Resolved again in " + printSeconds(t1 - t0) + "s");
            }
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    public static String printSeconds(long millis) {
        return millis / 1000 + "." + (millis % 1000)/100;
    }

    @Test
    public void kademliaFindNodeLimitTest() {
        PeerId us = new HostBuilder().generateIdentity().getPeerId();
        KademliaEngine kad = new KademliaEngine(Multihash.fromBase58(us.toBase58()),
                new RamProviderStore(1000), new RamRecordStore(), Optional.of(new RamBlockstore()));
        RamAddressBook addrs = new RamAddressBook();
        kad.setAddressBook(addrs);
        for (int i=0; i < 1000; i++) {
            PeerId peer = new HostBuilder().generateIdentity().getPeerId();
            for (int j=0; j < 100; j++) {
                kad.addIncomingConnection(peer);
                addrs.addAddrs(peer, 0, new Multiaddr[]{new Multiaddr("/ip4/127.0.0.1/tcp/4001/p2p/" + peer.toBase58())});
            }
        }
        List<PeerAddresses> closest = kad.getKClosestPeers(new byte[32], 20);
        Assert.assertTrue(closest.size() <= 20);
        for (PeerAddresses addr : closest) {
            Assert.assertTrue(addr.addresses.size() == 1);
        }
    }
}
