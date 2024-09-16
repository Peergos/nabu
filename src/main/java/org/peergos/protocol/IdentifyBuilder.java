package org.peergos.protocol;

import identify.pb.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multiformats.Protocol;
import io.libp2p.core.multistream.*;
import io.libp2p.etc.types.*;
import io.libp2p.protocol.*;

import java.net.*;
import java.util.*;
import java.util.stream.*;
import java.util.stream.Stream;

public class IdentifyBuilder {

    public static void addIdentifyProtocol(Host node, List<Multiaddr> announceAddresses) {
        IdentifyOuterClass.Identify.Builder identifyBuilder = IdentifyOuterClass.Identify.newBuilder()
                .setProtocolVersion("ipfs/0.1.0")
                .setAgentVersion("nabu/v0.1.0")
                .setPublicKey(ByteArrayExtKt.toProtobuf(node.getPrivKey().publicKey().bytes()))
                .addAllListenAddrs(Stream.concat(node.listenAddresses().stream(), announceAddresses.stream())
                        .flatMap(a -> expandWildcardAddresses(a).stream())
                        .map(Multiaddr::serialize)
                        .map(ByteArrayExtKt::toProtobuf)
                        .collect(Collectors.toList()));

        for (ProtocolBinding<?> protocol : node.getProtocols()) {
            identifyBuilder = identifyBuilder.addAllProtocols(protocol.getProtocolDescriptor().getAnnounceProtocols());
        }
        Identify identify = new Identify(identifyBuilder.build());
        node.addProtocolHandler(identify);
    }

    /* /ip6/::/tcp/4001 should expand to the following for example:
    "/ip6/0:0:0:0:0:0:0:1/udp/4001/quic"
    "/ip4/50.116.48.246/tcp/4001"
    "/ip4/127.0.0.1/tcp/4001"
    "/ip6/2600:3c03:0:0:f03c:92ff:fee7:bc1c/tcp/4001"
    "/ip6/0:0:0:0:0:0:0:1/tcp/4001"
    "/ip4/50.116.48.246/udp/4001/quic"
    "/ip4/127.0.0.1/udp/4001/quic"
    "/ip6/2600:3c03:0:0:f03c:92ff:fee7:bc1c/udp/4001/quic"
     */
    public static List<Multiaddr> expandWildcardAddresses(Multiaddr addr) {
        // Do not include /p2p or /ipfs components which are superfluous here
        if (! isWildcard(addr))
            return List.of(new Multiaddr(addr.getComponents()
                    .stream()
                    .filter(c -> c.getProtocol() != Protocol.P2P
                            && c.getProtocol() != Protocol.IPFS)
                    .collect(Collectors.toList())));
        if (addr.has(Protocol.IP4))
            return listNetworkAddresses(false, addr);
        if (addr.has(Protocol.IP6)) // include IP4
            return listNetworkAddresses(true, addr);
        return Collections.emptyList();
    }

    public static List<Multiaddr> listNetworkAddresses(boolean includeIp6, Multiaddr addr) {
        try {
            return Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
                    .flatMap(net -> net.getInterfaceAddresses().stream()
                            .map(InterfaceAddress::getAddress)
                            .filter(ip -> includeIp6 || ip instanceof Inet4Address))
                    .map(ip -> new Multiaddr(Stream.concat(Stream.of(new MultiaddrComponent(ip instanceof Inet4Address ?
                                            Protocol.IP4 :
                                            Protocol.IP6, ip.getAddress())),
                                    addr.getComponents().stream()
                                            .filter(c -> c.getProtocol() != Protocol.IP4
                                                    && c.getProtocol() != Protocol.IP6
                                                    && c.getProtocol() != Protocol.P2P
                                                    && c.getProtocol() != Protocol.IPFS))
                            .collect(Collectors.toList())))
                    .collect(Collectors.toList());
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isWildcard(Multiaddr addr) {
        String s = addr.toString();
        return s.contains("/::/") || s.contains("/0:0:0:0/");
    }
}
