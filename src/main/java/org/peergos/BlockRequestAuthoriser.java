package org.peergos;

import io.ipfs.cid.*;

import java.util.concurrent.*;

public interface BlockRequestAuthoriser {

    CompletableFuture<Boolean> allowRead(Cid block, byte[] blockData, Cid sourceNodeId, String auth);
}
