package org.peergos.blockstore;

import io.ipfs.cid.Cid;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class FilteredBlockstore implements Blockstore {

    private final Blockstore blocks;
    public FilteredBlockstore(Blockstore blocks) {
        this.blocks = blocks;
    }

    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        //not implemented
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        return blocks.has(c);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid c) {
        return blocks.get(c);
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        return blocks.put(block, codec);
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid c) {
        return blocks.rm(c);
    }

    @Override
    public CompletableFuture<List<Cid>> refs() {
        return blocks.refs();
    }
}
