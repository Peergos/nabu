package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.bitswap.*;
import org.peergos.dht.*;

import java.util.*;

public class KademliaTest {

    @Test
    @Ignore
    public void bootstrap() {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        Kademlia dht = new Kademlia(new KademliaEngine());
        Host node1 = Server.buildHost(10000 + new Random().nextInt(50000), List.of(bitswap1, dht));
        node1.start().join();
        try {
            Multiaddr bootstrapNode = Multiaddr.fromString("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt");
            KademliaController bootstrap = dht.dial(node1, bootstrapNode).getController().join();
        } finally {
            node1.stop();
        }
    }
}
