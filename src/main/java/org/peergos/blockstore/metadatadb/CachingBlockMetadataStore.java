package org.peergos.blockstore.metadatadb;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;
import org.peergos.blockstore.Blockstore;
import org.peergos.util.Futures;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

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

    private void cacheBlockMetadata(byte[] block, boolean isRaw) {
        Cid cid = hashToCid(block, isRaw);
        metadata.put(cid, block);
    }

    private Cid hashToCid(byte[] input, boolean isRaw) {
        return buildCid(Hash.sha256(input), isRaw);
    }

    private Cid buildCid(byte[] sha256, boolean isRaw) {
        return Cid.buildCidV1(isRaw ? Cid.Codec.Raw : Cid.Codec.DagCbor, Multihash.Type.sha2_256, sha256);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid hash) {
        return target.get(hash).thenApply(bopt -> {
            bopt.ifPresent(b -> cacheBlockMetadata(b, hash.codec == Cid.Codec.Raw));
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
    public CompletableFuture<List<Cid>> refs() {
        return target.refs();
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
}
