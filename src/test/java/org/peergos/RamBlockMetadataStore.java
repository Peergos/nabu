package org.peergos;

import io.ipfs.cid.Cid;
import org.peergos.blockstore.metadatadb.BlockMetadata;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class RamBlockMetadataStore implements BlockMetadataStore {

    private final Map<Cid, BlockMetadata> store;

    public RamBlockMetadataStore() {
        this.store = new HashMap<>(50_000);
    }

    @Override
    public Optional<BlockMetadata> get(Cid block) {
        return Optional.ofNullable(store.get(block));
    }

    @Override
    public void put(Cid block, BlockMetadata meta) {
        store.put(block, meta);
    }

    @Override
    public void remove(Cid block) {
        store.remove(block);
    }

    @Override
    public boolean applyToAll(Consumer<Cid> action) {
        store.keySet().stream().forEach(action);
        return true;
    }

    @Override
    public Stream<Cid> list() {
        return store.keySet().stream();
    }

    @Override
    public Stream<Cid> listCbor() {
        return store.keySet()
                .stream()
                .filter(c -> c.codec != Cid.Codec.Raw);
    }

    @Override
    public long size() {
        return store.size();
    }

    @Override
    public void compact() {}
}
