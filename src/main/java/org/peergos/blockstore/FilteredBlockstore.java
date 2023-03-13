package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import java.util.concurrent.CompletableFuture;

public interface FilteredBlockstore extends Blockstore {

    CompletableFuture<Boolean> bloomAdd(Cid cid);
}
