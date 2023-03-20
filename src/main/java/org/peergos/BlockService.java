package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;

import java.util.*;

public interface BlockService {

    List<byte[]> get(List<Cid> hashes, Set<PeerId> peers);

    default byte[] get(Cid c, Set<PeerId> peers) {
        return get(Collections.singletonList(c), peers).get(0);
    }
}
