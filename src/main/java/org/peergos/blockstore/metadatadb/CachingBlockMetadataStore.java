package org.peergos.blockstore.metadatadb;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;
import org.peergos.blockstore.Blockstore;
import org.peergos.util.Futures;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.*;

public class CachingBlockMetadataStore implements Blockstore {

    private final Blockstore target;
    private final BlockMetadataStore metadata;

    public CachingBlockMetadataStore(Blockstore target, BlockMetadataStore metadata) {
        this.target = target;
        this.metadata = metadata;
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        return target.put(block, codec)
                .thenApply(cid -> {
                        metadata.put(cid, block);
                    return cid;
                });
    }

    @Override
    public CompletableFuture<BlockMetadata> getBlockMetadata(Cid block) {
        Optional<BlockMetadata> meta = metadata.get(block);
        if (meta.isPresent())
            return Futures.of(meta.get());
        return target.getBlockMetadata(block)
                .thenApply(blockmeta -> {
                    metadata.put(block, blockmeta);
                    return blockmeta;
                });
    }

    private void cacheBlockMetadata(byte[] block, Cid.Codec codec) {
        Cid cid = hashToCid(block, codec);
        metadata.put(cid, block);
    }

    private Cid hashToCid(byte[] input, Cid.Codec codec) {
        return buildCid(Hash.sha256(input), codec);
    }

    private Cid buildCid(byte[] sha256, Cid.Codec codec) {
        return Cid.buildCidV1(codec, Multihash.Type.sha2_256, sha256);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid hash) {
        return target.get(hash).thenApply(bopt -> {
            bopt.ifPresent(b -> cacheBlockMetadata(b, hash.codec));
            return bopt;
        });
    }

    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        Optional<BlockMetadata> meta = metadata.get(c);
        if (meta.isPresent())
            return Futures.of(true);
        return get(c).thenApply(opt -> opt.isPresent());
    }

    @Override
    public CompletableFuture<Boolean> hasAny(Multihash h) {
        return Futures.of(Stream.of(Cid.Codec.DagCbor, Cid.Codec.Raw, Cid.Codec.DagProtobuf)
                .anyMatch(c -> has(new Cid(1, c, h.getType(), h.getHash())).join()));
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        return target.bloomAdd(cid);
    }

    @Override
    public CompletableFuture<List<Cid>> refs(boolean useBlockstore) {
        if (useBlockstore)
            return target.refs(true);
        return CompletableFuture.completedFuture(metadata.list().collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid c) {
        return target.rm(c).thenApply(res -> {
            if (res) {
                metadata.remove(c);
            }
            return res;
        });
    }

    public void updateMetadataStoreIfEmpty() {
        if (metadata.size() > 0)
            return;
        List<Cid> cids = target.refs(true).join();
        for(Cid c : cids) {
            Optional<BlockMetadata> existing = metadata.get(c);
            if (existing.isEmpty())
                metadata.put(c, target.getBlockMetadata(c).join());
        }
    }
}
