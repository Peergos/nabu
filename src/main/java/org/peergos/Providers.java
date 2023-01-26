package org.peergos;

import org.peergos.protocol.dht.pb.*;

import java.util.*;
import java.util.stream.*;

public class Providers {
    public final List<PeerAddresses> providers, closerPeers;

    public Providers(List<PeerAddresses> providers, List<PeerAddresses> closerPeers) {
        this.providers = providers;
        this.closerPeers = closerPeers;
    }

    public static Providers fromProtobuf(Dht.Message reply) {
        return new Providers(
                reply.getProviderPeersList().stream()
                        .map(PeerAddresses::fromProtobuf)
                        .collect(Collectors.toList()),
                reply.getCloserPeersList().stream()
                        .map(PeerAddresses::fromProtobuf)
                        .collect(Collectors.toList()));
    }
}
