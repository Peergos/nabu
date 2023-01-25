package org.peergos.protocol.dht;

import io.libp2p.core.multistream.*;

public class Kademlia extends StrictProtocolBinding<KademliaController> {
    public Kademlia(KademliaEngine dht, boolean localOnly) {
        super("/ipfs/" + (localOnly ? "lan/" : "") + "kad/1.0.0", new KademliaProtocol(dht));
    }
}
