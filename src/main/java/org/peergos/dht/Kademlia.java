package org.peergos.dht;

import io.libp2p.core.multistream.*;

public class Kademlia extends StrictProtocolBinding<KademliaController> {
    public Kademlia(KademliaEngine dht) {
        super("/ipfs/kad/1.0.0", new KademliaProtocol(dht));
    }
}
