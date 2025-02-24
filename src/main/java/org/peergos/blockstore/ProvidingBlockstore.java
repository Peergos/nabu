package org.peergos.blockstore;

import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import org.peergos.blockstore.metadatadb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.*;

public class ProvidingBlockstore implements Blockstore {

    private final Blockstore target;
    public final BlockingDeque<Cid> toPublish = new LinkedBlockingDeque<>();

    public ProvidingBlockstore(Blockstore target) {
        this.target = target;
    }

    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        return target.has(c);
    }

    @Override
    public CompletableFuture<Boolean> hasAny(Multihash h) {
        return target.hasAny(h);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid c) {
        return target.get(c);
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        CompletableFuture<Cid> res = target.put(block, codec);
        res.thenApply(toPublish::add);
        return res;
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid c) {
        return target.rm(c);
    }

    @Override
    public CompletableFuture<List<Cid>> refs(boolean useBlockstore) {
        return target.refs(useBlockstore);
    }

    @Override
    public CompletableFuture<Long> count(boolean useBlockstore) {
        return target.count(useBlockstore);
    }

    @Override
    public CompletableFuture<Boolean> applyToAll(Consumer<Cid> action, boolean useBlockstore) {
        return target.applyToAll(action, useBlockstore);
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        return target.bloomAdd(cid);
    }

    @Override
    public CompletableFuture<BlockMetadata> getBlockMetadata(Cid h) {
        return target.getBlockMetadata(h);
    }
}
