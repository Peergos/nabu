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

    CompletableFuture<Boolean> has(Cid c);

    CompletableFuture<Optional<byte[]>> get(Cid c);

    CompletableFuture<Cid> put(byte[] block, Cid.Codec codec);
}
