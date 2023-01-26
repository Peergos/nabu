package org.peergos;

import com.google.protobuf.*;
import identify.pb.*;
import io.ipfs.api.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.dht.pb.*;

import java.util.*;
import java.util.stream.*;

public class KademliaTest {

    @Test
    public void bootstrap() throws Exception {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        Kademlia lanDht = new Kademlia(new KademliaEngine(), true);
        Kademlia wanDht = new Kademlia(new KademliaEngine(), false);
        Ping ping = new Ping();
        Host node1 = Server.buildHost(10000 + new Random().nextInt(50000),
                List.of(ping, bitswap1, lanDht, wanDht));
        node1.start().join();

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
            KademliaController bootstrap = dht.dial(node1, address2).getController().join();
            List<PeerAddresses> peers = bootstrap.closerPeers(Cid.cast(node2.getPeerId().getBytes())).join();
            Optional<PeerAddresses> matching = peers.stream()
                    .filter(p -> Arrays.equals(p.peerId.toBytes(), node2.getPeerId().getBytes()))
                    .findFirst();
            if (matching.isEmpty())
                throw new IllegalStateException("Couldn't find node2 from kubo!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
