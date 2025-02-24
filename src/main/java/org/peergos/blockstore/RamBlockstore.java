package org.peergos.blockstore;

import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import org.peergos.*;
import org.peergos.blockstore.metadatadb.BlockMetadata;
import org.peergos.cbor.*;
import org.peergos.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.*;

public class RamBlockstore implements Blockstore {

    private final ConcurrentHashMap<Cid, byte[]> blocks = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        return CompletableFuture.completedFuture(blocks.containsKey(c));
    }

    @Override
    public CompletableFuture<Boolean> hasAny(Multihash h) {
        return Futures.of(Stream.of(Cid.Codec.DagCbor, Cid.Codec.Raw, Cid.Codec.DagProtobuf)
                .anyMatch(c -> has(new Cid(1, c, h.getType(), h.getHash())).join()));
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid c) {
        return CompletableFuture.completedFuture(Optional.ofNullable(blocks.get(c)));
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));
        blocks.put(cid, block);
        return CompletableFuture.completedFuture(cid);
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid c) {
        if (blocks.containsKey(c)) {
            blocks.remove(c);
            return CompletableFuture.completedFuture(true);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        //not implemented
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<List<Cid>> refs(boolean useBlockstore) {
        return CompletableFuture.completedFuture(new ArrayList(blocks.keySet()));
    }

    @Override
    public CompletableFuture<Long> count(boolean useBlockstore) {
        return Futures.of((long)blocks.size());
    }

    @Override
    public CompletableFuture<Boolean> applyToAll(Consumer<Cid> action, boolean useBlockstore) {
        blocks.keySet().stream().forEach(action);
        return Futures.of(true);
    }

    @Override
    public CompletableFuture<BlockMetadata> getBlockMetadata(Cid h) {
        byte[] block = get(h).join().get();
        return Futures.of(new BlockMetadata(block.length, CborObject.getLinks(h, block)));
    }
}
