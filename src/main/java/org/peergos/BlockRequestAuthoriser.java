package org.peergos;

import io.ipfs.cid.*;

import java.util.concurrent.*;

public interface BlockRequestAuthoriser {

    CompletableFuture<Boolean> allowRead(Cid block, Cid sourceNodeId, String auth);
}
