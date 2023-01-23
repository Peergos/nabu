package org.peergos;

import identify.pb.*;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.dsl.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.mux.*;
import io.libp2p.crypto.keys.*;
import io.libp2p.etc.types.*;
import io.libp2p.protocol.*;
import io.libp2p.security.noise.*;
import io.libp2p.transport.tcp.*;
import org.peergos.bitswap.*;
import org.peergos.http.*;

import java.net.*;
import java.util.*;

public class Server {

    public static void main(String[] args) throws Exception {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        InetSocketAddress unusedProxyTarget = new InetSocketAddress("localhost", 7000);
        Host node1 = buildHost(6001, bitswap1, Optional.of(unusedProxyTarget));
        node1.start().join();
        System.out.println("Node 1 started and listening on " + node1.listenAddresses());

        RamBlockstore blockstore2 = new RamBlockstore();
        Bitswap bitswap2 = new Bitswap(new BitswapEngine(blockstore2));
        InetSocketAddress proxyTarget = new InetSocketAddress("localhost", 8000);
        Host node2 = buildHost(7001, bitswap2, Optional.of(proxyTarget));
        node2.start().join();
        System.out.println("Node 2 started and listening on " + node2.listenAddresses());

        Multiaddr address2 = node2.listenAddresses().get(0);

        System.out.println("Stopping nodes...");
        node1.stop().get();
        node2.stop().get();
    }

    public static Host buildHost(int listenPort,
                                 Bitswap bitswap,
                                 Optional<SocketAddress> httpProxyTarget) {
        PrivKey privKey = RsaKt.generateRsaKeyPair(2048).getFirst();
        PeerId peerId = PeerId.fromPubKey(privKey.publicKey());
        Multiaddr advertisedAddr = Multiaddr.fromString("/ip4/127.0.0.1/tcp/" + listenPort).withP2P(peerId);
        return buildHost(privKey, List.of("/ip4/127.0.0.1/tcp/" + listenPort), advertisedAddr, bitswap, httpProxyTarget);
    }

    public static Host buildHost(PrivKey privKey,
                                 List<String> listenAddrs,
                                 Multiaddr advertisedAddr,
                                 Bitswap bitswap,
                                 Optional<SocketAddress> httpProxyTarget) {
        return BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
            b.getMuxers().add(StreamMuxerProtocol.getMplex());

            Ping ping = new Ping();
            b.getProtocols().add(ping);
            b.getProtocols().add(bitswap);
            IdentifyOuterClass.Identify.Builder identifyBuilder = IdentifyOuterClass.Identify.newBuilder()
                    .setProtocolVersion("ipfs/0.1.0")
                    .setAgentVersion("nabu/v0.1.0")
                    .setPublicKey(ByteArrayExtKt.toProtobuf(privKey.publicKey().bytes()))
                    .addListenAddrs(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()))
                    .setObservedAddr(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()))
                    .addAllProtocols(ping.getProtocolDescriptor().getAnnounceProtocols())
                    .addAllProtocols(bitswap.getProtocolDescriptor().getAnnounceProtocols());
            if (httpProxyTarget.isPresent()) {
                HttpProtocol.Binding httpProxy = new HttpProtocol.Binding(httpProxyTarget.get());
                b.getProtocols().add(httpProxy);
                identifyBuilder = identifyBuilder.addAllProtocols(httpProxy.getProtocolDescriptor().getAnnounceProtocols());
            }
            b.getProtocols().add(new Identify(identifyBuilder.build()));

            for (String listenAddr : listenAddrs) {
                b.getNetwork().listen(listenAddr);
            }

            b.getConnectionHandlers().add(conn -> System.out.println(conn.localAddress() +
                    " received connection from " + conn.remoteAddress() +
                    " on transport " + conn.transport()));
        });
    }
}