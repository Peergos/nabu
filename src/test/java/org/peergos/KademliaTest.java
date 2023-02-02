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

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class KademliaTest {

    @Test
    public void dhtMessages() throws Exception {
        RamBlockstore blockstore1 = new RamBlockstore();
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore1);
        Host node1 = builder1.build();
        node1.start().join();
        io.ipfs.multihash.Multihash node1Id = Multihash.deserialize(node1.getPeerId().getBytes());

        // connect node 2 to kubo, but not node 1
        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore());
        Host node2 = builder2.build();
        node2.start().join();

        try {
            IPFS kubo = new IPFS("localhost", 5001);
            String kuboID = (String)kubo.id().get("ID");
            io.ipfs.multihash.Multihash kuboId = Cid.fromBase58(kuboID);
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kuboID);
            builder2.getBitswap().get().dial(node2, address2).getController().join();

            Kademlia dht = builder1.getWanDht().get();
            KademliaController bootstrap1 = dht.dial(node1, address2).getController().join();
            Multihash peerId2 = Multihash.deserialize(node2.getPeerId().getBytes());
            List<PeerAddresses> peers = bootstrap1.closerPeers(peerId2).join();
            Optional<PeerAddresses> matching = peers.stream()
                    .filter(p -> Arrays.equals(p.peerId.toBytes(), node2.getPeerId().getBytes()))
                    .findFirst();
            if (matching.isEmpty())
                throw new IllegalStateException("Couldn't find node2 from kubo!");

            Cid block = blockstore1.put("Provide me.".getBytes(), Cid.Codec.Raw).join();
            bootstrap1.provide(block, PeerAddresses.fromHost(node1)).join();

            Providers providers = bootstrap1.getProviders(block).join();
            Optional<PeerAddresses> matchingProvider = providers.providers.stream()
                    .filter(p -> Arrays.equals(p.peerId.toBytes(), node1.getPeerId().getBytes()))
                    .findFirst();
            if (matchingProvider.isEmpty())
                throw new IllegalStateException("Node1 is not a provider for block!");

            // publish a cid in kubo ipns
//            Map publish = kubo.name.publish(block);
//            Cid kuboPeerId = new Cid(1, Cid.Codec.Libp2pKey, kuboId.getType(), kuboId.getHash());
//            GetResult kuboIpnsGet = wanDht.dial(node1, address2).getController().join().getValue(kuboId).join();
//            LinkedBlockingDeque<PeerAddresses> queue = new LinkedBlockingDeque<>();
//            queue.addAll(kuboIpnsGet.closerPeers);
//            outer: for (int i=0; i < 1000; i++) {
//                if (kuboIpnsGet.record.isPresent())
//                    break;
//                PeerAddresses closer = queue.poll();
//                List<String> candidates = closer.addresses.stream()
//                        .map(MultiAddress::toString)
//                        .filter(a -> a.contains("tcp") && a.contains("ip4") && !a.contains("127.0.0.1") && !a.contains("/172."))
//                        .collect(Collectors.toList());
//                for (String candidate: candidates) {
//                    try {
//                        kuboIpnsGet = wanDht.dial(node1, Multiaddr.fromString(candidate + "/p2p/" + closer.peerId)).getController().join()
//                                .getValue(kuboId).join();
//                        queue.addAll(kuboIpnsGet.closerPeers);
//                        continue outer;
//                    } catch (Exception e) {
//                        System.out.println(e.getMessage());
//                    }
//                }
//            }

//            GetResult join = dht.dial(node1, node2.listenAddresses().get(0)).getController().join().getValue(kuboPeerId).join();

            // Do a dht lookup for ourself
//            List<PeerAddresses> closerPeers = dht.dial(node1, address2).getController().join().closerPeers(node1Id).join();
//            LinkedBlockingDeque<PeerAddresses> queue = new LinkedBlockingDeque<>();
//            queue.addAll(closerPeers);
//            outer: for (int i=0; i < 100; i++) {
//                PeerAddresses closer = queue.poll();
//                List<String> candidates = closer.addresses.stream()
//                        .map(MultiAddress::toString)
//                        .filter(a -> a.contains("tcp") && a.contains("ip4") && !a.contains("127.0.0.1") && !a.contains("/172."))
//                        .collect(Collectors.toList());
//                for (String candidate: candidates) {
//                    try {
//                        closerPeers = dht.dial(node1, Multiaddr.fromString(candidate + "/p2p/" + closer.peerId)).getController().join()
//                                .closerPeers(node1Id).join();
//                        queue.addAll(closerPeers);
//                        continue outer;
//                    } catch (Exception e) {
//                        System.out.println(e.getMessage());
//                    }
//                }
//            }

            // sign an ipns record to publish
            String pathToPublish = "/ipfs/" + block;
            LocalDateTime expiry = LocalDateTime.now().plusHours(1);
            int sequence = 1;
            long ttl = 3600_000_000_000L;

            System.out.println("Sending put value...");
            boolean success = bootstrap1.putValue(pathToPublish, expiry, sequence, ttl, node1Id, node1.getPrivKey()).join();
            if (! success)
                throw new IllegalStateException("Failed to publish IPNS record!");
            GetResult getresult = bootstrap1.getValue(node1Id).join();
            if (! getresult.record.isPresent())
                throw new IllegalStateException("Kubo didn't return our published IPNS record!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
