package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.jetbrains.annotations.*;
import org.peergos.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;
import java.util.stream.Stream;

public class RamAddressBook implements AddressBook {

    /** A peer legitimately announces a handful of addresses. Holding more than this means we are
     *  accumulating addresses a peer has moved off, or that someone is filling our book for us. */
    public static final int MAX_ADDRESSES_PER_PEER = 32;
    public static final long DEFAULT_TTL_MILLIS = 7 * 24 * 3600_000L;

    /** Last time each address was announced to us, so we can drop the ones that have gone quiet. */
    private final Map<PeerId, Map<Multiaddr, Long>> addresses;
    private final long ttlMillis;
    private final Supplier<Long> time;

    public RamAddressBook() {
        this(DEFAULT_TTL_MILLIS, System::currentTimeMillis);
    }

    public RamAddressBook(long ttlMillis, Supplier<Long> time) {
        this.addresses = Collections.synchronizedMap(new LRUCache<>(10_000));
        this.ttlMillis = ttlMillis;
        this.time = time;
    }

    @NotNull
    @Override
    public CompletableFuture<Void> addAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
        addresses.putIfAbsent(peerId, new HashMap<>());
        Map<Multiaddr, Long> val = addresses.get(peerId);
        synchronized (val) {
            long now = time.get();
            for (Multiaddr addr : withoutPeerId(multiaddrs, peerId))
                val.put(addr, now);
            prune(val);
        }
        return CompletableFuture.completedFuture(null);
    }

    @NotNull
    @Override
    public CompletableFuture<Collection<Multiaddr>> getAddrs(@NotNull PeerId peerId) {
        Map<Multiaddr, Long> val = addresses.get(peerId);
        if (val == null)
            return CompletableFuture.completedFuture(Collections.emptyList());
        synchronized (val) {
            prune(val);
            return CompletableFuture.completedFuture(new ArrayList<>(val.keySet()));
        }
    }

    @NotNull
    @Override
    public CompletableFuture<Void> setAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
        Map<Multiaddr, Long> val = new HashMap<>();
        long now = time.get();
        for (Multiaddr addr : withoutPeerId(multiaddrs, peerId))
            val.put(addr, now);
        prune(val);
        addresses.put(peerId, val);
        return CompletableFuture.completedFuture(null);
    }

    /** Drop addresses we haven't seen announced within the ttl, then the oldest of whatever is left
     *  until we are within the per peer cap. Callers hold the lock on val. */
    private void prune(Map<Multiaddr, Long> val) {
        long cutoff = time.get() - ttlMillis;
        val.values().removeIf(lastSeen -> lastSeen < cutoff);
        if (val.size() <= MAX_ADDRESSES_PER_PEER)
            return;
        List<Map.Entry<Multiaddr, Long>> oldestFirst = new ArrayList<>(val.entrySet());
        oldestFirst.sort(Map.Entry.comparingByValue());
        for (int i = 0; i < oldestFirst.size() - MAX_ADDRESSES_PER_PEER; i++)
            val.remove(oldestFirst.get(i).getKey());
    }

    private static List<Multiaddr> withoutPeerId(Multiaddr[] in, PeerId id) {
        byte[] peerId = id.getBytes();
        return Stream.of(in)
                .map(a -> withoutPeerId(a, peerId))
                .collect(Collectors.toList());
    }

    private static Multiaddr withoutPeerId(Multiaddr in, byte[] peerId) {
        List<MultiaddrComponent> comp = in.getComponents();
        MultiaddrComponent last = comp.get(comp.size() - 1);
        if ((last.getProtocol() == Protocol.P2P || last.getProtocol() == Protocol.IPFS) &&
                Arrays.equals(last.getValue(), peerId))
            return new Multiaddr(comp.subList(0, comp.size() - 1));
        return in;
    }
}
