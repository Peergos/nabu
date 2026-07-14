package org.peergos.protocol.circuit;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.protocol.circuit.RelayTransport.CandidateRelay;
import org.peergos.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class Relay {

    /**
     * A candidate-relay source for AutoRelay, matching how kubo/go-libp2p discover relays: circuit-relay-v2
     * service is enabled by default on most nodes, so rather than looking relays up under a rendezvous key
     * we simply offer the peers we already know - those we are connected to, plus the DHT peers closest to
     * our own id - and let the relay transport probe each by attempting a reservation (non-relays are
     * skipped). Relayed peers are excluded.
     */
    public static Function<Host, List<CandidateRelay>> candidateRelaySource(Kademlia dht) {
        return us -> {
            Map<Multihash, List<Multiaddr>> byPeer = new LinkedHashMap<>();
            // peers we are directly connected to (cheap - already reachable)
            for (Connection conn : us.getNetwork().getConnections()) {
                Multiaddr remote = conn.remoteAddress();
                if (remote == null || remote.toString().contains("p2p-circuit"))
                    continue;
                Multihash peer = Multihash.deserialize(conn.secureSession().getRemoteId().getBytes());
                byPeer.computeIfAbsent(peer, k -> new ArrayList<>());
            }
            // DHT peers closest to our own id
            Multihash ourId = Multihash.deserialize(us.getPeerId().getBytes());
            try {
                for (PeerAddresses pa : dht.findClosestPeers(ourId, 20, us))
                    byPeer.computeIfAbsent(pa.peerId, k -> new ArrayList<>()).addAll(pa.getPublicAddresses());
            } catch (Exception e) {
                // best effort: fall back to just the connected peers
            }

            List<CandidateRelay> relays = new ArrayList<>();
            for (Map.Entry<Multihash, List<Multiaddr>> entry : byPeer.entrySet()) {
                if (entry.getKey().equals(ourId))
                    continue;
                PeerId peerId = PeerId.fromBase58(entry.getKey().toBase58());
                List<Multiaddr> addrs = new ArrayList<>(entry.getValue());
                if (addrs.isEmpty())
                    addrs.addAll(us.getAddressBook().getAddrs(peerId).join());
                addrs = addrs.stream()
                        .filter(a -> ! a.toString().contains("p2p-circuit"))
                        .distinct()
                        .collect(Collectors.toList());
                if (! addrs.isEmpty())
                    relays.add(new CandidateRelay(peerId, addrs));
            }
            return relays;
        };
    }
}
