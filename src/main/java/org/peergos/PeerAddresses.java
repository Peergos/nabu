package org.peergos;

import com.google.protobuf.*;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import org.peergos.protocol.dht.pb.*;

import java.io.*;
import java.util.*;
import java.util.stream.*;

public class PeerAddresses {
    public final Multihash peerId;
    public final List<MultiAddress> addresses;

    public PeerAddresses(Multihash peerId, List<MultiAddress> addresses) {
        this.peerId = peerId;
        this.addresses = addresses;
    }

    public List<MultiAddress> getPublicAddresses() {
        return addresses.stream().filter(a -> a.isPublic(false)).collect(Collectors.toList());
    }

    public static PeerAddresses fromProtobuf(Dht.Message.Peer peer) {
        Multihash peerId = Multihash.deserialize(peer.getId().toByteArray());
        List<MultiAddress> addrs = peer.getAddrsList()
                .stream()
                .map(b -> new MultiAddress(b.toByteArray()))
                .collect(Collectors.toList());
        return new PeerAddresses(peerId, addrs);
    }

    public Dht.Message.Peer toProtobuf() {
        return Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(peerId.toBytes()))
                .addAllAddrs(addresses.stream()
                        .map(a -> ByteString.copyFrom(a.getBytes()))
                        .collect(Collectors.toList()))
                .build();
    }

    public static PeerAddresses fromHost(Host host) {
        Multihash peerId = Multihash.deserialize(host.getPeerId().getBytes());
        List<MultiAddress> addrs = host.listenAddresses()
                .stream()
                .map(b -> new MultiAddress(b.serialize()))
                .collect(Collectors.toList());
        return new PeerAddresses(peerId, addrs);
    }

    @Override
    public String toString() {
        return peerId + ": " + addresses;
    }
}
