package org.peergos.blockstore;

import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import org.peergos.blockstore.metadatadb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class ProvidingBlockstore implements Blockstore {

    private final Blockstore target;
    private final BlockMetadataStore metaDB;
    public final BlockingDeque<Cid> toPublish = new LinkedBlockingDeque<>();

    public ProvidingBlockstore(Blockstore target, BlockMetadataStore metaDB) {
        this.target = target;
        this.metaDB = metaDB;
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
    public CompletableFuture<List<Cid>> refs() {
        return CompletableFuture.completedFuture(metaDB.list().collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        return target.bloomAdd(cid);
    }

    @Override
    public CompletableFuture<BlockMetadata> getBlockMetadata(Cid h) {
        return CompletableFuture.completedFuture(metaDB.get(h).get());
    }
}
