package org.peergos.protocol.dht;

import com.offbynull.kademlia.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import org.peergos.*;
import org.peergos.protocol.dnsaddr.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class Kademlia extends StrictProtocolBinding<KademliaController> {
    public static final int BOOTSTRAP_PERIOD_MILLIS = 300_000;
    private final KademliaEngine engine;

    public Kademlia(KademliaEngine dht, boolean localOnly) {
        super("/ipfs/" + (localOnly ? "lan/" : "") + "kad/1.0.0", new KademliaProtocol(dht));
        this.engine = dht;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
    }

    public int bootstrapRoutingTable(Host host, List<MultiAddress> addrs, Predicate<String> filter) {
        List<String> resolved = addrs.stream()
                .flatMap(a -> DnsAddr.resolve(a.toString()).stream())
                .filter(filter)
                .collect(Collectors.toList());
        List<? extends CompletableFuture<? extends KademliaController>> futures = resolved.stream()
                .parallel()
                .map(addr -> dial(host, Multiaddr.fromString(addr)).getController())
                .collect(Collectors.toList());
        int successes = 0;
        for (CompletableFuture<? extends KademliaController> future : futures) {
            try {
                future.orTimeout(5, TimeUnit.SECONDS).join();
                successes++;
            } catch (Exception e) {}
        }
        return successes;
    }

    public void startBootstrapThread(Host us) {
        new Thread(() -> {
            while (true) {
                try {
                    bootstrap(us);
                    Thread.sleep(BOOTSTRAP_PERIOD_MILLIS);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }, "Kademlia bootstrap").start();
    }

    public void bootstrap(Host us) {
        // lookup a random peer id
        byte[] hash = new byte[36];
        new Random().nextBytes(hash);
        hash[0] = 8;
        hash[1] = 1;
        hash[2] = 18;
        hash[3] = 32;
        Multihash randomPeerId = new Multihash(Multihash.Type.id, hash);
        findClosestPeers(randomPeerId, 20, us);
        // lookup our own peer id to keep our nearest neighbours uptodate
        findClosestPeers(Multihash.deserialize(us.getPeerId().getBytes()), 20, us);
    }

    static class RoutingEntry {
        public final Id key;
        public final PeerAddresses addresses;

        public RoutingEntry(Id key, PeerAddresses addresses) {
            this.key = key;
            this.addresses = addresses;
        }
    }

    public List<PeerAddresses> findClosestPeers(Multihash peerIdkey, int maxCount, Host us) {
        byte[] key = peerIdkey.toBytes();
        Id keyId = Id.create(key, 256);
        SortedSet<RoutingEntry> closest = new TreeSet<>((a, b) -> b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId));
        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId));
        closest.addAll(engine.getKClosestPeers(key).stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        toQuery.addAll(closest);
        Set<Multihash> queried = new HashSet<>();
        int queryParallelism = 3;
        while (true) {
            List<RoutingEntry> queryThisRound = toQuery.stream().limit(queryParallelism).collect(Collectors.toList());
            queryThisRound.forEach(r -> queried.add(r.addresses.peerId));
            List<CompletableFuture<List<PeerAddresses>>> futures = queryThisRound.stream()
                    .parallel()
                    .map(r -> getCloserPeers(peerIdkey, r.addresses, us))
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (CompletableFuture<List<PeerAddresses>> future : futures) {
                List<PeerAddresses> result = future.join();
                for (PeerAddresses peer : result) {
                    if (! queried.contains(peer.peerId)) {
                        queried.add(peer.peerId);
                        RoutingEntry e = new RoutingEntry(Id.create(Hash.sha256(peer.peerId.toBytes()), 256), peer);
                        toQuery.add(e);
                        closest.add(e);
                        foundCloser = true;
                    }
                }
            }
            // if now new peers in top k were returned we are done
            if (! foundCloser)
                break;
        }
        return closest.stream()
                .limit(maxCount).map(r -> r.addresses)
                .collect(Collectors.toList());
    }

    private CompletableFuture<List<PeerAddresses>> getCloserPeers(Multihash peerIDKey, PeerAddresses target, Host us) {
        Multiaddr[] multiaddrs = target.addresses.stream()
                .map(a -> Multiaddr.fromString(a.toString()))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
        return dial(us, PeerId.fromBase58(target.peerId.toBase58()), multiaddrs)
                .getController()
                .thenCompose(c -> c.closerPeers(peerIDKey))
                .orTimeout(2, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    e.printStackTrace();
                    return Collections.emptyList();
                });
    }
}
