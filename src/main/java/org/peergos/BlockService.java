package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;

import java.util.*;

public interface BlockService {

    List<HashedBlock> get(List<Want> hashes, Set<PeerId> peers, boolean addToBlockstore);

    default HashedBlock get(Want c, Set<PeerId> peers, boolean addToBlockstore) {
        return get(Collections.singletonList(c), peers, addToBlockstore).get(0);
    }
}
