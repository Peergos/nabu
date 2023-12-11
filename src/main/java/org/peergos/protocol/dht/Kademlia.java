package org.peergos.protocol.dht;

import com.offbynull.kademlia.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.etc.types.*;
import io.libp2p.protocol.*;
import org.peergos.*;
import org.peergos.protocol.dnsaddr.*;
import org.peergos.protocol.ipns.*;
import org.peergos.util.Logging;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;
import java.util.stream.Stream;

public class Kademlia extends StrictProtocolBinding<KademliaController> implements AddressBookConsumer {

    private static final Logger LOG = Logging.LOG();
    public static final int BOOTSTRAP_PERIOD_MILLIS = 300_000;
    public static final String WAN_DHT_ID = "/ipfs/kad/1.0.0";
    public static final String LAN_DHT_ID = "/ipfs/lan/kad/1.0.0";
    private final KademliaEngine engine;
    private final boolean localDht;
    private AddressBook addressBook;

    public Kademlia(KademliaEngine dht, boolean localOnly) {
        super(localOnly ? LAN_DHT_ID : WAN_DHT_ID, new KademliaProtocol(dht));
        this.engine = dht;
        this.localDht = localOnly;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
        this.addressBook = addrs;
    }

    public int bootstrapRoutingTable(Host host, List<MultiAddress> addrs, Predicate<String> filter) {
        List<String> resolved = addrs.stream()
                .parallel()
                .flatMap(a -> {
                    try {
                        return DnsAddr.resolve(a.toString()).stream();
                    } catch (CompletionException ce) {
                        return Stream.empty();
                    }
                })
                .filter(filter)
                .collect(Collectors.toList());
        List<? extends CompletableFuture<? extends KademliaController>> futures = resolved.stream()
                .parallel()
                .map(addr -> {
                    Multiaddr addrWithPeer = Multiaddr.fromString(addr);
                    addressBook.setAddrs(addrWithPeer.getPeerId(), 0, addrWithPeer);
                    return dial(host, addrWithPeer).getController();
                })
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

    private boolean connectTo(Host us, PeerAddresses peer) {
        try {
            new Identify().dial(us, PeerId.fromBase58(peer.peerId.toBase58()), getPublic(peer)).getController().join().id().join();
            return true;
        } catch (Exception e) {
            if (e.getCause() instanceof NothingToCompleteException || e.getCause() instanceof NonCompleteException)
                LOG.fine("Couldn't connect to " + peer.peerId);
            else
                e.printStackTrace();
            return false;
        }
    }
    public void bootstrap(Host us) {
        // lookup a random peer id
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        Multihash randomPeerId = new Multihash(Multihash.Type.sha2_256, hash);
        findClosestPeers(randomPeerId, 20, us);

        // lookup our own peer id to keep our nearest neighbours up-to-date,
        // and connect to all of them, so they know about our addresses
        List<PeerAddresses> closestToUs = findClosestPeers(Multihash.deserialize(us.getPeerId().getBytes()), 20, us);
        int connectedClosest = 0;
        for (PeerAddresses peer : closestToUs) {
            if (connectTo(us, peer))
                connectedClosest++;
        }
        LOG.info("Bootstrap connected to " + connectedClosest + " nodes close to us.");
    }

    static class RoutingEntry {
        public final Id key;
        public final PeerAddresses addresses;

        public RoutingEntry(Id key, PeerAddresses addresses) {
            this.key = key;
            this.addresses = addresses;
        }
    }

    private int compareKeys(RoutingEntry a, RoutingEntry b, Id keyId) {
        int prefixDiff = b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId);
        if (prefixDiff != 0)
            return prefixDiff;
        return a.addresses.peerId.toBase58().compareTo(b.addresses.peerId.toBase58());
    }

    public List<PeerAddresses> findClosestPeers(Multihash peerIdkey, int maxCount, Host us) {
        byte[] key = peerIdkey.toBytes();
        Id keyId = Id.create(Hash.sha256(key), 256);
        SortedSet<RoutingEntry> closest = Collections.synchronizedSortedSet(new TreeSet<>((a, b) -> compareKeys(a, b, keyId)));
        SortedSet<RoutingEntry> toQuery = Collections.synchronizedSortedSet(new TreeSet<>((a, b) -> compareKeys(a, b, keyId)));
        List<PeerAddresses> localClosest = engine.getKClosestPeers(key);
        if (maxCount == 1) {
            Collection<Multiaddr> existing = addressBook.get(PeerId.fromBase58(peerIdkey.toBase58())).join();
            if (! existing.isEmpty())
                return Collections.singletonList(new PeerAddresses(peerIdkey, new ArrayList<>(existing)));
            Optional<PeerAddresses> match = localClosest.stream().filter(p -> p.peerId.equals(peerIdkey)).findFirst();
            if (match.isPresent())
                return Collections.singletonList(match.get());
        }
        closest.addAll(localClosest.stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        toQuery.addAll(closest);
        Set<Multihash> queried = Collections.synchronizedSet(new HashSet<>());
        int queryParallelism = 3;
        while (true) {
            List<CompletableFuture<List<PeerAddresses>>> futures = toQuery.stream()
                    .limit(queryParallelism)
                    .parallel()
                    .map(r -> {
                        toQuery.remove(r);
                        queried.add(r.addresses.peerId);
                        return getCloserPeers(peerIdkey, r.addresses, us);
                    })
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (CompletableFuture<List<PeerAddresses>> future : futures) {
                try {
                    List<PeerAddresses> result = future.join();
                    for (PeerAddresses peer : result) {
                        if (!queried.contains(peer.peerId)) {
                            // exit early if we are looking for the specific node
                            if (maxCount == 1 && peer.peerId.equals(peerIdkey))
                                return Collections.singletonList(peer);
                            queried.add(peer.peerId);
                            Id peerKey = Id.create(Hash.sha256(peer.peerId.toBytes()), 256);
                            RoutingEntry e = new RoutingEntry(peerKey, peer);
                            toQuery.add(e);
                            closest.add(e);
                            foundCloser = true;
                        }
                    }
                } catch (Exception e) {
                    // couldn't contact peer
                }
            }
            // if no new peers in top k were returned we are done
            if (! foundCloser)
                break;
        }
        return closest.stream()
                .limit(maxCount).map(r -> r.addresses)
                .collect(Collectors.toList());
    }

    public CompletableFuture<List<PeerAddresses>> findProviders(Multihash block, Host us, int desiredCount) {
        byte[] key = block.bareMultihash().toBytes();
        Id keyId = Id.create(key, 256);
        List<PeerAddresses> providers = new ArrayList<>();
        providers.addAll(engine.getProviders(block));

        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId));
        toQuery.addAll(engine.getKClosestPeers(key).stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));

        Set<Multihash> queried = new HashSet<>();
        int queryParallelism = 3;
        while (true) {
            if (providers.size() >= desiredCount)
                return CompletableFuture.completedFuture(providers);
            List<RoutingEntry> queryThisRound = toQuery.stream().limit(queryParallelism).collect(Collectors.toList());
            toQuery.removeAll(queryThisRound);
            queryThisRound.forEach(r -> queried.add(r.addresses.peerId));
            List<CompletableFuture<Providers>> futures = queryThisRound.stream()
                    .parallel()
                    .map(r -> {
                        KademliaController res = null;
                        try {
                            res = dialPeer(r.addresses, us).join();
                            return res.getProviders(block).orTimeout(2, TimeUnit.SECONDS);
                        }catch (Exception e) {
                            return null;
                        }
                    }).filter(prov -> prov != null)
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (CompletableFuture<Providers> future : futures) {
                try {
                    Providers newProviders = future.join();
                    providers.addAll(newProviders.providers);
                    for (PeerAddresses peer : newProviders.closerPeers) {
                        if (!queried.contains(peer.peerId)) {
                            queried.add(peer.peerId);
                            RoutingEntry e = new RoutingEntry(Id.create(Hash.sha256(peer.peerId.toBytes()), 256), peer);
                            toQuery.add(e);
                            foundCloser = true;
                        }
                    }
                } catch (Exception e) {
                    if (! (e.getCause() instanceof TimeoutException))
                        e.printStackTrace();
                }
            }
            // if no new peers in top k were returned we are done
            if (! foundCloser)
                break;
        }

        return CompletableFuture.completedFuture(providers);
    }

    private CompletableFuture<List<PeerAddresses>> getCloserPeers(Multihash peerIDKey, PeerAddresses target, Host us) {
        try {
            return dialPeer(target, us).orTimeout(2, TimeUnit.SECONDS).join().closerPeers(peerIDKey);
        } catch (Exception e) {
            // we can't dial quic only nodes until it's implemented
            if (target.addresses.stream().allMatch(a -> a.toString().contains("quic")))
                return CompletableFuture.completedFuture(Collections.emptyList());
            if (e.getCause() instanceof NothingToCompleteException || e.getCause() instanceof NonCompleteException) {
                LOG.fine("Couldn't dial " + peerIDKey + " addrs: " + target.addresses);
            }  else if (e.getCause() instanceof TimeoutException)
                LOG.fine("Timeout dialing " + peerIDKey + " addrs: " + target.addresses);
            else if (e.getCause() instanceof ConnectionClosedException) {}
            else
                e.printStackTrace();
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    private Multiaddr[] getPublic(PeerAddresses target) {
        return target.addresses.stream()
                .filter(a -> localDht || PeerAddresses.isPublic(a, false))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
    }

    private CompletableFuture<? extends KademliaController> dialPeer(PeerAddresses target, Host us) {
        Multiaddr[] multiaddrs = target.addresses.stream()
                .map(a -> Multiaddr.fromString(a.toString()))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
        return dial(us, PeerId.fromBase58(target.peerId.toBase58()), multiaddrs).getController();
    }

    public CompletableFuture<Void> provideBlock(Multihash block, Host us, PeerAddresses ourAddrs) {
        List<PeerAddresses> closestPeers = findClosestPeers(block, 20, us);
        List<CompletableFuture<Boolean>> provides = closestPeers.stream()
                .parallel()
                .map(p -> dialPeer(p, us)
                        .thenCompose(contr -> contr.provide(block, ourAddrs))
                        .exceptionally(t -> {
                            if (t.getCause() instanceof NonCompleteException)
                                return true;
                            LOG.log(Level.FINE, t, t::getMessage);
                            return true;
                        }))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(provides.toArray(new CompletableFuture[0]));
    }

    public CompletableFuture<Void> publishIpnsValue(PrivKey priv,
                                                    Multihash publisher,
                                                    Multihash value,
                                                    long sequence,
                                                    Host us) {
        int hours = 1;
        LocalDateTime expiry = LocalDateTime.now().plusHours(hours);
        long ttlNanos = hours * 3600_000_000_000L;
        byte[] publishValue = ("/ipfs/" + value).getBytes();
        return publishValue(priv, publisher, publishValue, sequence, expiry, ttlNanos, us);
    }

    public CompletableFuture<Void> publishValue(PrivKey priv,
                                                Multihash publisher,
                                                byte[] publishValue,
                                                long sequence,
                                                LocalDateTime expiry,
                                                long ttlNanos,
                                                Host us) {
        int publishes = 0;
        for (int i=0; i < 5 && publishes < 20; i++) {
            List<PeerAddresses> closestPeers = findClosestPeers(publisher, 25, us);
            publishes += closestPeers.stream().parallel().mapToInt(peer -> {
                try {
                    boolean success = dialPeer(peer, us).join()
                            .putValue(publishValue, expiry, sequence,
                                    ttlNanos, publisher, priv).join();
                    if (success)
                        return 1;
                } catch (Exception e) {}
                return 0;
            }).sum();
        }
        return CompletableFuture.completedFuture(null);
    }

    public static Predicate<IpnsRecord> getNRecords(int minResults, CompletableFuture<byte[]> res) {
        List<IpnsRecord> candidates = new ArrayList<>();
        return rec -> {
            candidates.add(rec);
            if (candidates.size() >= minResults) {
                // Validate and sort records by sequence number
                List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
                res.complete(records.get(records.size() - 1).value);
                return false;
            }
            return true;
        };
    }

    public CompletableFuture<String> resolveIpnsValue(Multihash publisher, Host us, int minResults) {
        CompletableFuture<byte[]> res = new CompletableFuture<>();
        resolveValue(publisher, us, getNRecords(minResults, res));
        return res.thenApply(String::new);
    }

    public void resolveValue(Multihash publisher, Host us, Predicate<IpnsRecord> getMore) {
        List<PeerAddresses> closestPeers = findClosestPeers(publisher, 20, us);
        Set<PeerAddresses> queryCandidates = new HashSet<>();
        Set<Multihash> queriedPeers = new HashSet<>();
        for (PeerAddresses peer : closestPeers) {
            if (queriedPeers.contains(peer.peerId))
                continue;
            queriedPeers.add(peer.peerId);
            try {
                GetResult res = dialPeer(peer, us).join().getValue(publisher).join();
                if (res.record.isPresent() && res.record.get().publisher.equals(publisher))
                    if ( !getMore.test(res.record.get().value))
                        return;
                queryCandidates.addAll(res.closerPeers);
            } catch (Exception e) {}
        }
    }
}
