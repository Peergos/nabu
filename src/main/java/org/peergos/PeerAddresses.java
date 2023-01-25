package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import org.peergos.protocol.dht.pb.*;

import java.util.*;
import java.util.stream.*;

public class PeerAddresses {
    public final Cid peerId;
    public final List<MultiAddress> addresses;

    public PeerAddresses(Cid peerId, List<MultiAddress> addresses) {
        this.peerId = peerId;
        this.addresses = addresses;
    }

    public static PeerAddresses fromProtobuf(Dht.Message.Peer peer) {
        Cid peerId = Cid.cast(peer.getId().toByteArray());
        List<MultiAddress> addrs = peer.getAddrsList()
                .stream()
                .map(b -> new MultiAddress(b.toByteArray()))
                .collect(Collectors.toList());
        return new PeerAddresses(peerId, addrs);
    }

    @Override
    public String toString() {
        return peerId + ": " + addresses;
    }
}
