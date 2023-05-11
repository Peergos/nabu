package org.peergos.blockstore;

import io.ipfs.cid.*;

import java.util.*;
import java.util.concurrent.*;

public class ProvidingBlockstore implements Blockstore {

    private final Blockstore target;
    public final Queue<Cid> toPublish = new LinkedBlockingDeque<>();

    public ProvidingBlockstore(Blockstore target) {
        this.target = target;
    }

    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        return target.has(c);
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
        return target.refs();
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        return target.bloomAdd(cid);
    }
}
