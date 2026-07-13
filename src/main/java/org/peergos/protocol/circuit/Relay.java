package org.peergos.protocol.circuit;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.protocol.circuit.RelayTransport.CandidateRelay;
import org.peergos.*;
import org.peergos.protocol.dht.*;

import java.nio.charset.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class Relay {
    // Provide this to advertise your relay
    public static final Multihash RELAY_RENDEZVOUS_NAMESPACE = new Multihash(Multihash.Type.sha2_256, Hash.sha256("/libp2p/relay".getBytes(StandardCharsets.UTF_8)));

    public static void advertise(Kademlia dht, Host us) {
        dht.provideBlock(RELAY_RENDEZVOUS_NAMESPACE, us, PeerAddresses.fromHost(us)).join();
    }

    public static List<PeerAddresses> findRelays(Kademlia dht, Host us) {
        return dht.findProviders(RELAY_RENDEZVOUS_NAMESPACE, us, 20).join();
    }

    /** A candidate-relay source that discovers relays advertised in the DHT, for use by RelayTransport. */
    public static Function<Host, List<CandidateRelay>> dhtRelaySource(Kademlia dht) {
        return us -> findRelays(dht, us).stream()
                .map(relay -> new CandidateRelay(
                        PeerId.fromBase58(relay.peerId.toBase58()),
                        relay.getPublicAddresses()))
                .collect(Collectors.toList());
    }
}
