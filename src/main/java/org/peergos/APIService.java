package org.peergos;
import io.ipfs.cid.Cid;
import org.peergos.blockstore.Blockstore;
import org.peergos.util.Version;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class APIService {

    public static final Version CURRENT_VERSION = Version.parse("0.0.1");
    public static final String API_URL = "/api/v0/";

    private final Blockstore store;

    public APIService(Blockstore store) {
        this.store = store;
    }

    public CompletableFuture<Optional<byte[]>> getBlock(Cid cid, boolean addToLocal) {
        return getBlock(cid, Optional.empty(), addToLocal);
    }
    public CompletableFuture<Optional<byte[]>> getBlock(Cid cid, Optional<String> auth) {
        return getBlock(cid, auth, false);
    }
    public CompletableFuture<Optional<byte[]>> getBlock(Cid cid, Optional<String> auth, boolean addToLocal) {
        return store.has(cid).thenCompose(has -> {
            if (has) {
                return store.get(cid).thenCompose(block -> {
                    //todo get using auth
                    return CompletableFuture.completedFuture(block);
                });
            } else {
                if (addToLocal) {
                    //once retrieved, add to local also
                }
                return CompletableFuture.completedFuture(Optional.empty()); // todo get from network
            }
        });
    }

    public CompletableFuture<Cid> putBlock(byte[] block, String format) {
        Cid.Codec codec = null;
        if (format.equals("raw")) {
            codec = Cid.Codec.Raw;
        } else if (format.equals("cbor")) {
            codec = Cid.Codec.DagCbor;
        } else {
            throw new IllegalArgumentException("only raw and cbor format supported");
        }
        return store.put(block, codec);
    }

    public CompletableFuture<Boolean> rmBlock(Cid cid) {
        return store.rm(cid);
    }

    public CompletableFuture<Boolean> hasBlock(Cid cid) {
        return store.has(cid);
    }
    public CompletableFuture<List<Cid>> getRefs() {
        return store.refs();
    }

    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        return store.bloomAdd(cid);
    }
}
