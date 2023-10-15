package org.peergos.protocol.dht;

import io.ipfs.multihash.*;
import org.peergos.*;
import org.peergos.util.*;

import java.util.*;

public class RamProviderStore implements ProviderStore {

    private final Map<Multihash, Set<PeerAddresses>> store;

    public RamProviderStore(int cacheSize) {
        store = new LRUCache<>(cacheSize);
    }

    @Override
    public synchronized void addProvider(Multihash m, PeerAddresses peer) {
        store.putIfAbsent(m, new HashSet<>());
        store.get(m).add(peer);
    }

    @Override
    public synchronized Set<PeerAddresses> getProviders(Multihash m) {
        return store.getOrDefault(m, Collections.emptySet());
    }
}
