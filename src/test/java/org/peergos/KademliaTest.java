package org.peergos;

import com.google.protobuf.*;
import identify.pb.*;
import io.ipfs.api.*;
import io.ipfs.cid.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.*;
import org.peergos.protocol.ipns.pb.*;

import java.time.*;
import java.util.*;

public class KademliaTest {

    @Test
    public void dhtMessages() throws Exception {
        RamBlockstore blockstore1 = new RamBlockstore();
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(blockstore1));
        Kademlia lanDht = new Kademlia(new KademliaEngine(), true);
        Kademlia wanDht = new Kademlia(new KademliaEngine(), false);
        Ping ping = new Ping();
        Host node1 = Server.buildHost(10000 + new Random().nextInt(50000),
                List.of(ping, bitswap1, lanDht, wanDht));
        node1.start().join();
        Cid node1Id = Cid.cast(node1.getPeerId().getBytes());

        // connect node 2 to kubo, but not node 1
        Bitswap bitswap2 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        Host node2 = Server.buildHost(10000 + new Random().nextInt(50000),
                List.of(ping, bitswap2,
                        new Kademlia(new KademliaEngine(), true),
                        new Kademlia(new KademliaEngine(), false)));
        node2.start().join();

        try {
            IPFS kubo = new IPFS("localhost", 5001);
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kubo.id().get("ID"));
            bitswap2.dial(node2, address2).getController().join();

            IdentifyOuterClass.Identify id = new Identify().dial(node1, address2).getController().join().id().join();
            Kademlia dht = id.getProtocolsList().contains("/ipfs/lan/kad/1.0.0") ? lanDht : wanDht;
            KademliaController bootstrap1 = dht.dial(node1, address2).getController().join();
            List<PeerAddresses> peers = bootstrap1.closerPeers(Cid.cast(node2.getPeerId().getBytes())).join();
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

            // sign an ipns record to publish
            String pathToPublish = "/ipfs/" + block;
            LocalDateTime expiry = LocalDateTime.now().plusDays(1);
            int sequence = 1;
            long ttl = 86400_000_000_000L;
            byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(pathToPublish, expiry,
                    Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl);
            String expiryString = expiry.atOffset(ZoneOffset.UTC).format(IPNS.rfc3339nano);
            byte[] record = Ipns.IpnsEntry.newBuilder()
                    .setSequence(sequence)
                    .setTtl(ttl)
                    .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                    .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                    .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                    .setData(ByteString.copyFrom(cborEntryData))
                    .setSignatureV2(ByteString.copyFrom(node1.getPrivKey().sign(IPNS.createSigV2Data(cborEntryData))))
                    .setPubKey(ByteString.copyFrom(node1.getPrivKey().publicKey().bytes())) // not needed with Ed25519
                    .build().toByteArray();
//            boolean success = bootstrap1.putValue(node1Id, record).join();
            GetResult getresult = bootstrap1.getValue(node1Id).join();
            System.out.println();
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
