package org.peergos;
import io.ipfs.cid.Cid;
import io.libp2p.core.*;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.TypeLimitedBlockstore;
import org.peergos.util.Version;

import java.util.*;
import java.util.stream.*;

public class APIService {

    public static final Version CURRENT_VERSION = Version.parse("0.0.1");
    public static final String API_URL = "/api/v0/";

    private final Blockstore store;
    private final BlockService remoteBlocks;

    public APIService(Blockstore store, BlockService remoteBlocks) {
        this.store = store;
        this.remoteBlocks = remoteBlocks;
    }

    public List<HashedBlock> getBlocks(List<Want> wants, Set<PeerId> peers, boolean addToLocal) {
        List<HashedBlock> blocksFound = new ArrayList<>();

        List<Want> local = new ArrayList<>();
        List<Want> remote = new ArrayList<>();

        for (Want w : wants) {
            if (store.has(w.cid).join())
                local.add(w);
            else
                remote.add(w);
        }
        local.stream()
                .map(w -> new HashedBlock(w.cid, store.get(w.cid).join().get()))
                .forEach(blocksFound::add);
        if (remote.isEmpty())
            return blocksFound;
        return java.util.stream.Stream.concat(
                        blocksFound.stream(),
                        remoteBlocks.get(remote, peers, addToLocal).stream())
                .collect(Collectors.toList());
    }

    public boolean accepts(Cid.Codec codec) {
        if (store instanceof TypeLimitedBlockstore) {
            TypeLimitedBlockstore tlbs = (TypeLimitedBlockstore) store;
            return tlbs.accepts(codec);
        } else {
            return true;
        }
    }
    
    public Cid putBlock(byte[] block, Cid.Codec codec) {
        return store.put(block, codec).join();
    }

    public Boolean rmBlock(Cid cid) {
        return store.rm(cid).join();
    }

    public Boolean hasBlock(Cid cid) {
        return store.has(cid).join();
    }
    public List<Cid> getRefs() {
        return store.refs().join();
    }

    public Boolean bloomAdd(Cid cid) {
        return store.bloomAdd(cid).join();
    }
}
