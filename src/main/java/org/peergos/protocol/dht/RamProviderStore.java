package org.peergos.protocol.dht;

import io.ipfs.multihash.Multihash;
import org.peergos.protocol.dht.pb.*;
import org.peergos.util.*;

import java.util.*;

public class RamProviderStore implements ProviderStore {

    private final Map<Multihash, Set<Dht.Message.Peer>> store;

    public RamProviderStore(int cacheSize) {
        store = new LRUCache<>(cacheSize);
    }

    @Override
    public synchronized void addProvider(Multihash m, Dht.Message.Peer peer) {
        store.putIfAbsent(m, new HashSet<>());
        store.get(m).add(peer);
    }

    @Override
    public synchronized Set<Dht.Message.Peer> getProviders(Multihash m) {
        return store.getOrDefault(m, Collections.emptySet());
    }
}
