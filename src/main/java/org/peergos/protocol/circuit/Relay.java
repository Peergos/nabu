package org.peergos.protocol.circuit;

import io.ipfs.multihash.*;
import io.libp2p.core.*;
import org.peergos.*;
import org.peergos.protocol.dht.*;

import java.nio.charset.*;
import java.util.*;

public class Relay {
    // Provide this to advertise your relay
    public static final Multihash RELAY_RENDEZVOUS_NAMESPACE = new Multihash(Multihash.Type.sha2_256, Hash.sha256("/libp2p/relay".getBytes(StandardCharsets.UTF_8)));

    public static void advertise(Kademlia dht, Host us) {
        dht.provideBlock(RELAY_RENDEZVOUS_NAMESPACE, us, PeerAddresses.fromHost(us)).join();
    }

    public static List<PeerAddresses> findRelays(Kademlia dht, Host us) {
        List<PeerAddresses> relays = dht.findProviders(RELAY_RENDEZVOUS_NAMESPACE, us, 20).join();
        return relays;
    }
}
