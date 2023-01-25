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

public class KademliaTest {

    @Test
    public void bootstrap() throws Exception {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        Kademlia lanDht = new Kademlia(new KademliaEngine(), true);
        Kademlia wanDht = new Kademlia(new KademliaEngine(), false);
        Ping ping = new Ping();
        Host node1 = Server.buildHost(10000 + new Random().nextInt(50000), List.of(ping, bitswap1, lanDht, wanDht));
        node1.start().join();
        try {
            IPFS kubo = new IPFS("localhost", 5001);
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kubo.id().get("ID"));

            byte[] hash = new byte[32];
            new Random(42).nextBytes(hash);
            Cid nodeToFind = new Cid(1, Cid.Codec.Libp2pKey, Multihash.Type.sha2_256, hash);
            IdentifyOuterClass.Identify id = new Identify().dial(node1, address2).getController().join().id().join();
            Kademlia dht = id.getProtocolsList().contains("/ipfs/lan/kad/1.0.0") ? lanDht : wanDht;
            KademliaController bootstrap = dht.dial(node1, address2).getController().join();
            bootstrap.send(Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.FIND_NODE)
                            .setKey(ByteString.copyFrom(nodeToFind.toBytes()))
                    .build());
            System.out.println();
        } finally {
            node1.stop();
        }
    }
}
