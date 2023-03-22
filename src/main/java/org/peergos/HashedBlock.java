package org.peergos;

import io.ipfs.cid.Cid;

public class HashedBlock {
    public final Cid hash;
    public final byte[] block;

    public HashedBlock(Cid hash, byte[] block) {
        this.hash = hash;
        this.block = block;
    }
}
