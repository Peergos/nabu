package org.peergos.protocol.circuit;

import com.google.protobuf.*;
import identify.pb.*;
import io.ipfs.multihash.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.peergos.*;
import org.peergos.protocol.dht.*;

import java.util.*;

public class Relay {
    public static PeerAddresses findRelay(Kademlia dht, Host us) {
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        List<PeerAddresses> nodes = dht.findClosestPeers(new Multihash(Multihash.Type.sha2_256, hash), 20, us);
        Optional<PeerAddresses> relay = nodes.stream().filter(p -> ! p.getPublicAddresses().isEmpty() && isRelay(p, us)).findAny();
        if (relay.isEmpty())
            throw new IllegalStateException("Couldn't find relay");
        return relay.get();
    }

    public static boolean isRelay(PeerAddresses p, Host us) {
        try {
            Multiaddr[] addrs = p.addresses.stream().map(a -> new Multiaddr(a.toString())).toArray(Multiaddr[]::new);
            IdentifyOuterClass.Identify id = new Identify().dial(us, PeerId.fromBase58(p.peerId.toBase58()), addrs).getController().join().id().join();
            ProtocolStringList protocols = id.getProtocolsList();
            return protocols.contains("/libp2p/circuit/relay/0.2.0/hop") && protocols.contains("/libp2p/circuit/relay/0.2.0/stop");
        } catch (Exception e) {
            return false;
        }
    }
}
