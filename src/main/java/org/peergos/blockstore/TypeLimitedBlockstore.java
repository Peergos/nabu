package org.peergos.blockstore;

import io.ipfs.cid.Cid;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class TypeLimitedBlockstore implements Blockstore {

    private final Blockstore blocks;
    private final Set<Cid.Codec> allowedCodecs;

    public TypeLimitedBlockstore(Blockstore blocks, Set<Cid.Codec> allowedCodecs) {
        this.blocks = blocks;
        this.allowedCodecs = allowedCodecs;
    }

    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.bloomAdd(cid);
        }
        throw new IllegalArgumentException("Unsupported codec: " + cid.codec);
    }

    @Override
    public CompletableFuture<Boolean> has(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.has(cid);
        }
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.get(cid);
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        if (allowedCodecs.contains(codec)) {
            return blocks.put(block, codec);
        }
        throw new IllegalArgumentException("Unsupported codec: " + codec);
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.rm(cid);
        }
        throw new IllegalArgumentException("Unsupported codec: " + cid.codec);
    }

    @Override
    public CompletableFuture<List<Cid>> refs() {
        return blocks.refs();
    }
}
