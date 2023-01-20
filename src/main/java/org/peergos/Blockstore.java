package org.peergos;

import io.ipfs.cid.*;

import java.util.*;
import java.util.concurrent.*;

public interface Blockstore {

    CompletableFuture<Boolean> has(Cid c);

    CompletableFuture<Optional<byte[]>> get(Cid c);

    CompletableFuture<Cid> put(byte[] block, Cid.Codec codec);
}
