package org.peergos.blockstore.metadatadb;

import io.ipfs.cid.Cid;

import java.util.List;

public class BlockMetadata {

    public final int size;
    public final List<Cid> links;

    public BlockMetadata(int size, List<Cid> links) {
        this.size = size;
        this.links = links;
    }
}
