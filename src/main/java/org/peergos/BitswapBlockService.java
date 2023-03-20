package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import org.peergos.protocol.bitswap.*;

import java.util.*;
import java.util.stream.*;

public class BitswapBlockService implements BlockService {

    private final Host us;
    private final Bitswap bitswap;

    public BitswapBlockService(Host us, Bitswap bitswap) {
        this.us = us;
        this.bitswap = bitswap;
    }

    @Override
    public List<HashedBlock> get(List<Want> hashes, Set<PeerId> peers, boolean addToBlockstore) {
        return bitswap.get(hashes, us, peers, addToBlockstore)
                .stream()
                .map(f -> f.join())
                .collect(Collectors.toList());
    }
}
