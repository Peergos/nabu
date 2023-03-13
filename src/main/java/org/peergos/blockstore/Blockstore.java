package org.peergos.blockstore;

import io.ipfs.cid.*;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;

import java.util.*;
import java.util.concurrent.*;

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

    CompletableFuture<Optional<byte[]>> get(Cid c);

    CompletableFuture<Cid> put(byte[] block, Cid.Codec codec);

    CompletableFuture<Boolean> rm(Cid c);

    CompletableFuture<List<Cid>> refs();

}
