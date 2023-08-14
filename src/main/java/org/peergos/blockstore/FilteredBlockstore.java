package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.*;
import org.peergos.util.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.*;

public class FilteredBlockstore implements Blockstore {

    private final Blockstore blocks;
    private final Filter filter;

    public FilteredBlockstore(Blockstore blocks, Filter filter) {
        this.blocks = blocks;
        this.filter = filter;
    }

    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        filter.add(cid);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        if (filter.has(c))
            return blocks.has(c);
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> hasAny(Multihash h) {
        return Futures.of(Stream.of(Cid.Codec.DagCbor, Cid.Codec.Raw, Cid.Codec.DagProtobuf)
                .anyMatch(c -> has(new Cid(1, c, h.getType(), h.getHash())).join()));
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid c) {
        if (filter.has(c))
            return blocks.get(c);
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        return blocks.put(block, codec)
                .thenApply(filter::add);
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid c) {
        return blocks.rm(c);
    }

    @Override
    public CompletableFuture<List<Cid>> refs() {
        return blocks.refs();
    }

    public static FilteredBlockstore bloomBased(Blockstore source, double falsePositiveRate) {
        return new FilteredBlockstore(source, CidBloomFilter.build(source, falsePositiveRate));
    }

    public static FilteredBlockstore infiniBased(Blockstore source, double falsePositiveRate) {
        return new FilteredBlockstore(source, CidInfiniFilter.build(source, falsePositiveRate));
    }
}
