package org.peergos;

import identify.pb.*;
import io.ipfs.api.*;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.ipns.*;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class KademliaTest {

    @Test
    public void findOurNode() throws IOException {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore1);
        Host node1 = builder1.build();
        node1.start().join();
        Kademlia dht = builder1.getWanDht().get();
        Multihash node1Id = Multihash.deserialize(node1.getPeerId().getBytes());

        try {
            IPFS kubo = new IPFS("localhost", 5001);
            String kuboID = (String)kubo.id().get("ID");
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kuboID);

            // Do a dht lookup for ourself
            List<PeerAddresses> closerPeers = dht.dial(node1, address2).getController().join().closerPeers(node1Id).join();
            LinkedBlockingDeque<PeerAddresses> queue = new LinkedBlockingDeque<>();
            queue.addAll(closerPeers);
            outer: for (int i=0; i < 20; i++) {
                PeerAddresses closer = queue.poll();
                List<String> candidates = closer.addresses.stream()
                        .map(MultiAddress::toString)
                        .filter(a -> a.contains("tcp") && a.contains("ip4") && !a.contains("127.0.0.1") && !a.contains("/172."))
                        .collect(Collectors.toList());
                for (String candidate: candidates) {
                    try {
                        closerPeers = dht.dial(node1, Multiaddr.fromString(candidate + "/p2p/" + closer.peerId)).getController().join()
                                .closerPeers(node1Id).join();
                        queue.addAll(closerPeers);
                        continue outer;
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
            Assert.assertTrue(queue.size() > 100);
        } finally {
            node1.stop();
        }
    }

    @Test
    public void findAnotherNode() throws Exception {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore1);
        Host node1 = builder1.build();
        node1.start().join();

        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore());
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

    @Test
    public void publishIPNSRecordToKubo() throws IOException {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore1);
        Host node1 = builder1.build();
        node1.start().join();
        Multihash node1Id = Multihash.deserialize(node1.getPeerId().getBytes());

        try {
            IPFS kubo = new IPFS("localhost", 5001);
            String kuboID = (String)kubo.id().get("ID");
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kuboID);
            Cid block = blockstore1.put("Provide me.".getBytes(), Cid.Codec.Raw).join();
            Kademlia dht = builder1.getWanDht().get();

            // sign an ipns record to publish
            String pathToPublish = "/ipfs/" + block;
            LocalDateTime expiry = LocalDateTime.now().plusHours(1);
            int sequence = 1;
            long ttl = 3600_000_000_000L;

            System.out.println("Sending put value...");
            boolean success = false;

            for (int i = 0; i < 10; i++) {
                try {
                    success = dht.dial(node1, address2).getController().join()
                            .putValue(pathToPublish, expiry, sequence, ttl, node1Id, node1.getPrivKey())
                            .orTimeout(2, TimeUnit.SECONDS).join();
                    break;
                } catch (Exception timeout) {
                }
            }
            if (! success)
                throw new IllegalStateException("Failed to publish IPNS record!");
            GetResult getresult = dht.dial(node1, address2).getController().join().getValue(node1Id).join();
            if (! getresult.record.isPresent())
                throw new IllegalStateException("Kubo didn't return our published IPNS record!");
        } finally {
            node1.stop();
        }
    }

    @Test
    @Ignore // Kubo publish call is super slow and flakey
    public void retrieveKuboPublishedIPNS() throws IOException {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore1);
        Host node1 = builder1.build();
        node1.start().join();

        try {
            IPFS kubo = new IPFS("localhost", 5001);
            String kuboIDString = (String)kubo.id().get("ID");
            Multihash kuboId = Multihash.fromBase58(kuboIDString);
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kuboIDString);

            // publish a cid in kubo ipns
            Cid block = blockstore1.put("Provide me.".getBytes(), Cid.Codec.Raw).join();
            Map publish = kubo.name.publish(block);
            Kademlia wanDht = builder1.getWanDht().get();
            GetResult kuboIpnsGet = wanDht.dial(node1, address2).getController().join().getValue(kuboId).join();
            LinkedBlockingDeque<PeerAddresses> queue = new LinkedBlockingDeque<>();
            queue.addAll(kuboIpnsGet.closerPeers);
            outer: for (int i=0; i < 100; i++) {
                if (kuboIpnsGet.record.isPresent())
                    break;
                PeerAddresses closer = queue.poll();
                List<String> candidates = closer.addresses.stream()
                        .map(MultiAddress::toString)
                        .filter(a -> a.contains("tcp") && a.contains("ip4") && !a.contains("127.0.0.1") && !a.contains("/172."))
                        .collect(Collectors.toList());
                for (String candidate: candidates) {
                    try {
                        kuboIpnsGet = wanDht.dial(node1, Multiaddr.fromString(candidate + "/p2p/" + closer.peerId)).getController().join()
                                .getValue(kuboId).join();
                        queue.addAll(kuboIpnsGet.closerPeers);
                        continue outer;
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
            }
            if (kuboIpnsGet.record.isEmpty())
                throw new IllegalStateException("Couldn't find kubo's IPNS record!");
        } finally {
            node1.stop();
        }
    }
}
