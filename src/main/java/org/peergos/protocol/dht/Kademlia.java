package org.peergos.protocol.dht;

import com.offbynull.kademlia.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multiformats.Protocol;
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
import java.util.concurrent.atomic.*;
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
        List<? extends Future<? extends KademliaController>> futures = resolved.stream()
                .map(addr -> {
                    Multiaddr addrWithPeer = Multiaddr.fromString(addr);
                    addressBook.setAddrs(addrWithPeer.getPeerId(), 0, addrWithPeer);
                    return ioExec.submit(() -> dial(host, addrWithPeer).getController().join());
                })
                .collect(Collectors.toList());
        int successes = 0;
        for (Future<? extends KademliaController> future : futures) {
            try {
                future.get(5, TimeUnit.SECONDS);
                successes++;
            } catch (Exception e) {}
        }
        return successes;
    }

    private AtomicBoolean running = new AtomicBoolean(false);
    public void startBootstrapThread(Host us) {
        running.set(true);
        new Thread(() -> {
            while (running.get()) {
                try {
                    bootstrap(us);
                    Thread.sleep(BOOTSTRAP_PERIOD_MILLIS);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }, "Kademlia bootstrap").start();
    }

    public void stopBootstrapThread() {
        running.set(false);
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
        List<Multihash> connected = new ArrayList<>();
        for (PeerAddresses peer : closestToUs) {
            if (connectTo(us, peer))
                connected.add(peer.peerId);
        }
        LOG.info("Bootstrap connected to " + connected.size() + " nodes close to us. " + connected.stream().map(Multihash::toString).sorted().limit(5).collect(Collectors.toList()));
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

    private final ExecutorService ioExec = Executors.newFixedThreadPool(16);

    public List<PeerAddresses> findClosestPeers(Multihash peerIdkey, int maxCount, Host us) {
        if (maxCount == 1) {
            Collection<Multiaddr> existing = addressBook.get(PeerId.fromBase58(peerIdkey.toBase58())).join();
            if (!existing.isEmpty())
                return Collections.singletonList(new PeerAddresses(peerIdkey, new ArrayList<>(existing)));
        }
        byte[] key = peerIdkey.toBytes();
        return findClosestPeers(key, maxCount, us);
    }
    public List<PeerAddresses> findClosestPeers(byte[] key, int maxCount, Host us) {
        Id keyId = Id.create(Hash.sha256(key), 256);
        SortedSet<RoutingEntry> closest = Collections.synchronizedSortedSet(new TreeSet<>((a, b) -> compareKeys(a, b, keyId)));
        SortedSet<RoutingEntry> toQuery = Collections.synchronizedSortedSet(new TreeSet<>((a, b) -> compareKeys(a, b, keyId)));
        List<PeerAddresses> localClosest = engine.getKClosestPeers(key, Math.max(6, maxCount));
        if (maxCount == 1) {
            Optional<PeerAddresses> match = localClosest.stream().filter(p -> Arrays.equals(p.peerId.toBytes(), key)).findFirst();
            if (match.isPresent())
                return Collections.singletonList(match.get());
        }
        closest.addAll(localClosest.stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        toQuery.addAll(closest);
        if (toQuery.isEmpty())
            LOG.info("Couldn't find any local peers in kademlia routing table");
        Set<Multihash> queried = Collections.synchronizedSet(new HashSet<>());
        int queryParallelism = 3;
        while (true) {
            List<RoutingEntry> thisRound = toQuery.stream()
                    .filter(r -> hasTransportOverlap(r.addresses)) // don't waste time trying to dial nodes we can't
                    .limit(queryParallelism)
                    .collect(Collectors.toList());
            List<Future<List<PeerAddresses>>> futures = thisRound.stream()
                    .map(r -> {
                        toQuery.remove(r);
                        queried.add(r.addresses.peerId);
                        return ioExec.submit(() -> getCloserPeers(key, r.addresses, us).join());
                    })
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (Future<List<PeerAddresses>> future : futures) {
                try {
                    List<PeerAddresses> result = future.get();
                    for (PeerAddresses peer : result) {
                        if (!queried.contains(peer.peerId)) {
                            // exit early if we are looking for the specific node
                            if (maxCount == 1 && Arrays.equals(peer.peerId.toBytes(), key))
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
        toQuery.addAll(engine.getKClosestPeers(key, 20).stream()
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
                        StreamPromise<? extends KademliaController> conn = null;
                        try {
                            conn = dialPeer(r.addresses, us);
                            return conn.getController().join()
                                    .getProviders(block).orTimeout(2, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            return null;
                        } finally {
                            if (conn != null)
                                conn.getStream().thenApply(s -> s.close());
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

    private CompletableFuture<List<PeerAddresses>> getCloserPeers(byte[] key, PeerAddresses target, Host us) {
        StreamPromise<? extends KademliaController> conn = null;
        try {
            conn = dialPeer(target, us);
            KademliaController contr = conn.getController().orTimeout(2, TimeUnit.SECONDS).join();
            return contr.closerPeers(key);
        } catch (Exception e) {
            // we can't dial quic only nodes until it's implemented
            if (target.addresses.stream().allMatch(a -> a.toString().contains("quic")))
                return CompletableFuture.completedFuture(Collections.emptyList());
            if (e.getCause() instanceof NothingToCompleteException || e.getCause() instanceof NonCompleteException) {
                LOG.fine("Couldn't dial " + target.peerId + " addrs: " + target.addresses);
            }  else if (e.getCause() instanceof TimeoutException)
                LOG.fine("Timeout dialing " + target.peerId + " addrs: " + target.addresses);
            else if (e.getCause() instanceof ConnectionClosedException) {}
            else
                e.printStackTrace();
        } finally {
            if (conn != null)
                conn.getStream().thenApply(s -> s.close());
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    private Multiaddr[] getPublic(PeerAddresses target) {
        return target.addresses.stream()
                .filter(a -> localDht || PeerAddresses.isPublic(a, false))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
    }

    private StreamPromise<? extends KademliaController> dialPeer(PeerAddresses target, Host us) {
        Multiaddr[] multiaddrs = target.addresses.stream()
                .map(a -> Multiaddr.fromString(a.toString()))
                .filter(a -> ! a.has(Protocol.DNS) && ! a.has(Protocol.DNS4) && ! a.has(Protocol.DNS6))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
        return dial(us, PeerId.fromBase58(target.peerId.toBase58()), multiaddrs);
    }

    public CompletableFuture<Void> provideBlock(Multihash block, Host us, PeerAddresses ourAddrs) {
        List<PeerAddresses> closestPeers = findClosestPeers(block, 20, us);
        List<CompletableFuture<Boolean>> provides = closestPeers.stream()
                .parallel()
                .map(p -> {
                    StreamPromise<? extends KademliaController> conn = dialPeer(p, us);
                    return conn.getController()
                            .thenCompose(contr -> contr.provide(block, ourAddrs))
                            .thenApply(res -> {
                                conn.getStream().thenApply(s -> s.close());
                                return res;
                            })
                            .exceptionally(t -> {
                                if (t.getCause() instanceof NonCompleteException)
                                    return true;
                                LOG.log(Level.FINE, t, t::getMessage);
                                conn.getStream().thenApply(s -> s.close());
                                return true;
                            });
                })
                .collect(Collectors.toList());
        return CompletableFuture.allOf(provides.toArray(new CompletableFuture[0]));
    }

    public CompletableFuture<Integer> publishIpnsValue(PrivKey priv,
                                                       Multihash publisher,
                                                       Multihash value,
                                                       long sequence,
                                                       Host us) {
        int hours = 1;
        LocalDateTime expiry = LocalDateTime.now().plusHours(hours);
        long ttlNanos = hours * 3600_000_000_000L;
        byte[] publishValue = ("/ipfs/" + value).getBytes();
        byte[] signedRecord = IPNS.createSignedRecord(publishValue, expiry, sequence, ttlNanos, priv);
        return publishValue(publisher, signedRecord, us);
    }

    private CompletableFuture<Boolean> putValue(Multihash publisher,
                                                byte[] signedRecord,
                                                PeerAddresses peer,
                                                Host us) {
        StreamPromise<? extends KademliaController> conn = null;
        try {
            conn = dialPeer(peer, us);
            return conn.getController().join()
                    .putValue(publisher, signedRecord);
        } catch (Exception e) {} finally {
            if (conn != null)
                conn.getStream().thenApply(s -> s.close());
        }
        return CompletableFuture.completedFuture(false);
    }

    private boolean hasTransportOverlap(PeerAddresses p) {
        return p.addresses.stream().anyMatch(a -> a.has(Protocol.TCP) && ! a.has(Protocol.P2PCIRCUIT));
    }

    public CompletableFuture<Integer> publishValue(Multihash publisher,
                                                   byte[] signedRecord,
                                                   Host us) {
        byte[] key = IPNS.getKey(publisher);
        Optional<IpnsMapping> parsed = IPNS.parseAndValidateIpnsEntry(key, signedRecord);
        if (parsed.isEmpty() || !parsed.get().publisher.equals(publisher))
            throw new IllegalStateException("Tried to publish invalid INS record for " + publisher);
        Optional<IpnsRecord> existing = engine.getRecord(publisher);
        // don't overwrite 'newer' record
        if (existing.isEmpty() || parsed.get().value.compareTo(existing.get()) > 0) {
            engine.addRecord(publisher, parsed.get().value);
        }

        Set<Multihash> publishes = Collections.synchronizedSet(new HashSet<>());
        int minPublishes = 30;

        Id keyId = Id.create(Hash.sha256(key), 256);
        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> compareKeys(a, b, keyId));
        List<PeerAddresses> localClosest = engine.getKClosestPeers(key, minPublishes);
        int queryParallelism = 3;
        toQuery.addAll(localClosest.stream()
                .limit(queryParallelism)
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        Set<Multihash> queried = Collections.synchronizedSet(new HashSet<>());
        while (! toQuery.isEmpty()) {
            int remaining = toQuery.size() - 3;
            List<RoutingEntry> thisRound = toQuery.stream()
                    .filter(r -> hasTransportOverlap(r.addresses)) // don't waste time trying to dial nodes we can't
                    .limit(queryParallelism)
                    .collect(Collectors.toList());
            List<? extends CompletableFuture<List<RoutingEntry>>> futures = thisRound.stream()
                    .map(r -> {
                        toQuery.remove(r);
                        queried.add(r.addresses.peerId);
                        return CompletableFuture.supplyAsync(() -> getCloserPeers(key, r.addresses, us).thenApply(res -> {
                            List<RoutingEntry> more = new ArrayList<>();
                            for (PeerAddresses peer : res) {
                                if (! queried.contains(peer.peerId)) {
                                    Id peerKey = Id.create(Hash.sha256(IPNS.getKey(peer.peerId)), 256);
                                    RoutingEntry e = new RoutingEntry(peerKey, peer);
                                    more.add(e);
                                }
                            }
                            CompletableFuture.supplyAsync(() -> putValue(publisher, signedRecord, r.addresses, us)
                                    .thenAccept(done -> {
                                        if (done)
                                            publishes.add(r.addresses.peerId);
                                    }), ioExec);
                            return more;
                        }).join(), ioExec);
                    })
                    .collect(Collectors.toList());
            futures.forEach(f -> {
                try {
                    if (publishes.size() >= minPublishes)
                        return;
                    toQuery.addAll(f.orTimeout(2, TimeUnit.SECONDS).join());
                } catch (Exception e) {}
            });
            // exit early if we have enough results
            if (publishes.size() >= minPublishes)
                break;
            if (toQuery.size() == remaining) {
                // publish to closest remaining nodes
                System.out.println("Publishing to further nodes, so far only " + publishes.size());
                while (publishes.size() < minPublishes && !toQuery.isEmpty()) {
                    List<RoutingEntry> closest = toQuery.stream()
                    .limit(minPublishes - publishes.size() + 5)
                    .collect(Collectors.toList());
                    List<? extends CompletableFuture<?>> lastFutures = closest.stream()
                            .map(r -> {
                                toQuery.remove(r);
                                queried.add(r.addresses.peerId);
                                return CompletableFuture.supplyAsync(() -> putValue(publisher, signedRecord, r.addresses, us)
                                        .thenAccept(done -> {
                                            if (done)
                                                publishes.add(r.addresses.peerId);
                                        }), ioExec);
                            })
                            .collect(Collectors.toList());
                    lastFutures.forEach(f -> {
                        try {
                            f.orTimeout(2, TimeUnit.SECONDS).join();
                        } catch (Exception e) {}
                    });
                }
                break;
            }
        }
        return CompletableFuture.completedFuture(publishes.size());
    }

    public CompletableFuture<String> resolveIpnsValue(Multihash publisher, Host us, int minResults) {
        List<IpnsRecord> candidates = resolveValue(publisher, minResults, us);
        List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        if (records.isEmpty())
            return CompletableFuture.failedFuture(new IllegalStateException("Couldn't find IPNS value for " + publisher));
        return CompletableFuture.completedFuture(new String(records.get(records.size() - 1).value));
    }

    private CompletableFuture<Optional<GetResult>> getValueFromPeer(PeerAddresses peer, Multihash publisher, Host us) {
        StreamPromise<? extends KademliaController> conn = null;
        try {
            conn = dialPeer(peer, us);
            return conn
                    .getController()
                    .orTimeout(1, TimeUnit.SECONDS)
                    .join()
                    .getValue(publisher)
                    .orTimeout(1, TimeUnit.SECONDS)
                    .thenApply(Optional::of);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(Optional.empty());
        } finally {
            if (conn != null)
                conn.getStream().thenApply(s -> s.close());
        }
    }
    public List<IpnsRecord> resolveValue(Multihash publisher, int minResults, Host us) {
        byte[] key = IPNS.getKey(publisher);
        List<IpnsRecord> candidates = Collections.synchronizedList(new ArrayList<>());
        Optional<IpnsRecord> local = engine.getRecord(publisher);
        local.ifPresent(candidates::add);

        Id keyId = Id.create(Hash.sha256(key), 256);
        SortedSet<RoutingEntry> toQuery = Collections.synchronizedSortedSet(new TreeSet<>((a, b) -> compareKeys(a, b, keyId)));
        List<PeerAddresses> localClosest = engine.getKClosestPeers(key, 20);
        int queryParallelism = 3;
        toQuery.addAll(localClosest.stream()
                .filter(p -> hasTransportOverlap(p)) // don't waste time trying to dial nodes we can't
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        Set<Multihash> queried = Collections.synchronizedSet(new HashSet<>());
        int countdown = 20;
        while (! toQuery.isEmpty()) {
            int remaining = toQuery.size() - 3;
            List<RoutingEntry> thisRound = toQuery.stream()
                    .limit(queryParallelism)
                    .collect(Collectors.toList());
            List<CompletableFuture<CompletableFuture<Void>>> futures = thisRound.stream()
                    .map(r -> {
                        toQuery.remove(r);
                        queried.add(r.addresses.peerId);
                        return CompletableFuture.supplyAsync(() -> getValueFromPeer(r.addresses, publisher, us).thenAccept(get ->
                                get.ifPresent(g -> {
                                    if (g.record.isPresent() && g.record.get().publisher.equals(publisher))
                                        candidates.add(g.record.get().value);
                                    for (PeerAddresses peer : g.closerPeers) {
                                        if (!queried.contains(peer.peerId) && hasTransportOverlap(peer)) {
                                            Id peerKey = Id.create(Hash.sha256(IPNS.getKey(peer.peerId)), 256);
                                            RoutingEntry e = new RoutingEntry(peerKey, peer);
                                            toQuery.add(e);
                                        }
                                    }
                                })), ioExec);
                    })
                    .collect(Collectors.toList());
            futures.forEach(f -> {
                try {
                    if (candidates.size() >= minResults)
                        return;
                    f.orTimeout(5, TimeUnit.SECONDS).join()
                            .orTimeout(5, TimeUnit.SECONDS).join();
                } catch (Exception e) {}
            });
            // exit early if we have enough results
            if (candidates.size() >= minResults)
                break;
            if (toQuery.size() == remaining)
                countdown--;
            if (countdown <= 0)
                break;
        }
        return candidates;
    }
}
