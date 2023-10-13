package org.peergos;

import com.google.protobuf.*;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multiformats.Protocol;
import org.peergos.protocol.dht.pb.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.*;

public class PeerAddresses {
    public final Multihash peerId;
    public final List<Multiaddr> addresses;

    public PeerAddresses(Multihash peerId, List<Multiaddr> addresses) {
        this.peerId = peerId;
        this.addresses = addresses;
    }

    public List<Multiaddr> getPublicAddresses() {
        return addresses.stream().filter(a -> isPublic(a, false)).collect(Collectors.toList());
    }

    public static boolean isPublic(Multiaddr a, boolean testReachable) {
        if (a.has(Protocol.IP6ZONE))
            return true;
        try {
            if (a.has(Protocol.IP4)) {
                InetAddress ip = InetAddress.getByName(a.getFirstComponent(Protocol.IP4).getStringValue());
                if (!ip.isLoopbackAddress() && !ip.isSiteLocalAddress() && !ip.isLinkLocalAddress() && !ip.isAnyLocalAddress()) {
                    return !testReachable || ip.isReachable(1000);
                }

                return false;
            }
            if (a.has(Protocol.IP6)) {
                InetAddress ip = InetAddress.getByName(a.getFirstComponent(Protocol.IP6).getStringValue());
                if (!ip.isLoopbackAddress() && !ip.isSiteLocalAddress() && !ip.isLinkLocalAddress() && !ip.isAnyLocalAddress()) {
                    return !testReachable || ip.isReachable(1000);
                }

                return false;
            }
        } catch (IOException e) {}
        return false;
    }

    public static PeerAddresses fromProtobuf(Dht.Message.Peer peer) {
        Multihash peerId = Multihash.deserialize(peer.getId().toByteArray());
        List<Multiaddr> addrs = peer.getAddrsList()
                .stream()
                .map(b -> Multiaddr.deserialize(b.toByteArray()))
                .collect(Collectors.toList());
        return new PeerAddresses(peerId, addrs);
    }

    public Dht.Message.Peer toProtobuf() {
        return Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(peerId.toBytes()))
                .addAllAddrs(addresses.stream()
                        .map(a -> ByteString.copyFrom(a.serialize()))
                        .collect(Collectors.toList()))
                .build();
    }

    public static PeerAddresses fromHost(Host host) {
        Multihash peerId = Multihash.deserialize(host.getPeerId().getBytes());
        List<Multiaddr> addrs = host.listenAddresses();
        return new PeerAddresses(peerId, addrs);
    }

    @Override
    public String toString() {
        return peerId + ": " + addresses;
    }
}
