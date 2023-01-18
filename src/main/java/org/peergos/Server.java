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

import java.util.*;

public class Server {

    public static void main(String[] args) throws Exception {
        Host node1 = buildHost(4001);
        node1.start().get();
        System.out.println("Node 1 started and listening on " + node1.listenAddresses());

        Host node2 = buildHost(7001);
        node2.start().get();
        System.out.println("Node 2 started and listening on " + node2.listenAddresses());

        Multiaddr address = node2.listenAddresses().get(0);
        PingController pinger = new Ping().dial(node1, address).getController().get();

        System.out.println("Sending 5 ping messages to " + address);
        for (int i = 0; i < 5; i++) {
            long latency = pinger.ping().get();
            System.out.println("Ping " + i + ", latency " + latency + "ms");
        }

        node1.stop().get();
        node2.stop().get();
    }

    public static Host buildHost(int listenPort) {
        PrivKey privKey = RsaKt.generateRsaKeyPair(2048).getFirst();
        PeerId peerId = PeerId.fromPubKey(privKey.publicKey());
        Multiaddr advertisedAddr = Multiaddr.fromString("/ip4/127.0.0.1/tcp/" + listenPort).withP2P(peerId);
        return buildHost(privKey, List.of("/ip4/127.0.0.1/tcp/" + listenPort), advertisedAddr);
    }

    public static Host buildHost(PrivKey privKey, List<String> listenAddrs, Multiaddr advertisedAddr) {
        return BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
            b.getMuxers().add(StreamMuxerProtocol.getMplex());

            Ping ping = new Ping();
            b.getProtocols().add(ping);
            b.getProtocols().add(new Identify(IdentifyOuterClass.Identify.newBuilder()
                    .setProtocolVersion("ipfs/0.1.0")
                    .setAgentVersion("nabu/v0.1.0")
                    .setPublicKey(ByteArrayExtKt.toProtobuf(privKey.publicKey().bytes()))
                    .addListenAddrs(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()))
                    .setObservedAddr(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()))
                    .addAllProtocols(ping.getProtocolDescriptor().getAnnounceProtocols())
                    .build()));

            for (String listenAddr : listenAddrs) {
                b.getNetwork().listen(listenAddr);
            }

            b.getConnectionHandlers().add(conn -> System.out.println(conn.localAddress() +
                    " received connection from " + conn.remoteAddress() +
                    " on transport " + conn.transport()));
        });
    }
}