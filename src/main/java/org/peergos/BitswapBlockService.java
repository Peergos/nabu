package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;

import java.util.*;
import java.util.stream.*;

public class BitswapBlockService implements BlockService {

    private final Host us;
    private final Bitswap bitswap;
    private final Kademlia dht;

    public BitswapBlockService(Host us, Bitswap bitswap, Kademlia dht) {
        this.us = us;
        this.bitswap = bitswap;
        this.dht = dht;
    }

    @Override
    public List<HashedBlock> get(List<Want> hashes, Set<PeerId> peers, boolean addToBlockstore) {
        if (peers.isEmpty()) {
            List<PeerAddresses> providers = dht.findProviders(hashes.get(0).cid, us, 5).join();
            peers = providers.stream()
                    .map(p -> PeerId.fromBase58(p.peerId.toBase58()))
                    .collect(Collectors.toSet());
        }
        return bitswap.get(hashes, us, peers, addToBlockstore)
                .stream()
                .map(f -> f.join())
                .collect(Collectors.toList());
    }
}
