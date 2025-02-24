package org.peergos.blockstore;

import io.ipfs.cid.*;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import org.peergos.blockstore.metadatadb.BlockMetadata;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

public interface Blockstore {

    default String hashToKey(Multihash hash) {
        String padded = new Base32().encodeAsString(hash.toBytes());
        int padStart = padded.indexOf("=");
        return padStart > 0 ? padded.substring(0, padStart) : padded;
    }

    default Cid keyToHash(String key) {
        byte[] decoded = new Base32().decode(key);
        return Cid.cast(decoded);
    }

    CompletableFuture<Boolean> has(Cid c);

    CompletableFuture<Boolean> hasAny(Multihash h);

    CompletableFuture<Optional<byte[]>> get(Cid c);

    CompletableFuture<Cid> put(byte[] block, Cid.Codec codec);

    CompletableFuture<Boolean> rm(Cid c);

    CompletableFuture<Long> count(boolean useBlockstore);

    CompletableFuture<List<Cid>> refs(boolean useBlockstore);

    CompletableFuture<Boolean> applyToAll(Consumer<Cid> action, boolean useBlockstore);

    CompletableFuture<Boolean> bloomAdd(Cid cid);

    CompletableFuture<BlockMetadata> getBlockMetadata(Cid h);
}