package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.*;
import org.peergos.blockstore.metadatadb.BlockMetadata;
import org.peergos.util.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.*;

public class FilteredBlockstore implements Blockstore {

    private final Blockstore blocks;
    private final Filter present;
    private volatile Filter absent;
    private final AtomicLong absentCount = new AtomicLong(0);

    public FilteredBlockstore(Blockstore blocks, Filter present) {
        this.blocks = blocks;
        this.present = present;
        this.absent = buildAbsentFilter();
    }

    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        present.add(cid);
        return CompletableFuture.completedFuture(true);
    }

    private static Filter buildAbsentFilter() {
        return CidInfiniFilter.build(1_000, 0.001);
    }

    private void addAbsentBlock(Cid c) {
        if (absentCount.get() > 10_000) {
            absentCount.set(0);
            absent = buildAbsentFilter();
        }
        absentCount.incrementAndGet();
        absent.add(c);
    }

    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        if (present.has(c) && ! absent.has(c))
            return blocks.has(c).thenApply(res -> {
                if (! res)
                    addAbsentBlock(c);
                return false;
            });
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> hasAny(Multihash h) {
        return Futures.of(Stream.of(Cid.Codec.DagCbor, Cid.Codec.Raw, Cid.Codec.DagProtobuf)
                .anyMatch(c -> has(new Cid(1, c, h.getType(), h.getHash())).join()));
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid c) {
        if (present.has(c) && ! absent.has(c)) {
            return blocks.get(c).exceptionally(t -> {
                addAbsentBlock(c);
                return Optional.empty();
            });
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        return blocks.put(block, codec)
                .thenApply(present::add);
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid c) {
        return blocks.rm(c);
    }

    @Override
    public CompletableFuture<List<Cid>> refs(boolean useBlockstore) {
        return blocks.refs(useBlockstore);
    }

    public static FilteredBlockstore bloomBased(Blockstore source, double falsePositiveRate) {
        return new FilteredBlockstore(source, CidBloomFilter.build(source, falsePositiveRate));
    }

    public static FilteredBlockstore infiniBased(Blockstore source, double falsePositiveRate) {
        return new FilteredBlockstore(source, CidInfiniFilter.build(source, falsePositiveRate));
    }

    @Override
    public CompletableFuture<BlockMetadata> getBlockMetadata(Cid h) {
        return blocks.getBlockMetadata(h);
    }

}
