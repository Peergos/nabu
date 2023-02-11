package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

public class RamAddressBook implements AddressBook {

    Map<PeerId, Set<Multiaddr>> addresses = new ConcurrentHashMap<>();

    @NotNull
    @Override
    public CompletableFuture<Void> addAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
        addresses.putIfAbsent(peerId, new HashSet<>());
        Set<Multiaddr> val = addresses.get(peerId);
        synchronized (val) {
            val.addAll(Arrays.asList(multiaddrs));
        }
        return CompletableFuture.completedFuture(null);
    }

    @NotNull
    @Override
    public CompletableFuture<Collection<Multiaddr>> getAddrs(@NotNull PeerId peerId) {
        return CompletableFuture.completedFuture(addresses.getOrDefault(peerId, Collections.emptySet()));
    }

    @NotNull
    @Override
    public CompletableFuture<Void> setAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
        Set<Multiaddr> val = new HashSet<>();
        val.addAll(Arrays.asList(multiaddrs));
        addresses.put(peerId, val);
        return CompletableFuture.completedFuture(null);
    }
}
