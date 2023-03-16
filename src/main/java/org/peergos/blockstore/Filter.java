package org.peergos.blockstore;

import io.ipfs.cid.*;

public interface Filter {

    boolean has(Cid c);

    /**
     *
     * @param c
     * @return the argument c
     */
    Cid add(Cid c);
}
