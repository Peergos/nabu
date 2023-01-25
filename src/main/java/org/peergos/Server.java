package org.peergos;

import identify.pb.*;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.dsl.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.core.mux.*;
import io.libp2p.crypto.keys.*;
import io.libp2p.etc.types.*;
import io.libp2p.protocol.*;
import io.libp2p.security.noise.*;
import io.libp2p.transport.tcp.*;
import org.peergos.bitswap.*;
import org.peergos.dht.*;

import java.util.*;

public class Server {

    public static void main(String[] args) throws Exception {
        Kademlia dht = new Kademlia(new KademliaEngine());
        Host node1 = buildHost(6001, List.of(
                new Ping(),
                new Bitswap(new BitswapEngine(new RamBlockstore())),
                dht));
        node1.start().join();
        System.out.println("Node 1 started and listening on " + node1.listenAddresses());

        Multiaddr bootstrapNode = Multiaddr.fromString("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt");
        KademliaController bootstrap = dht.dial(node1, bootstrapNode).getController().join();

        System.out.println("Stopping server...");
        node1.stop().get();
    }

    public static Host buildHost(int listenPort,
                                 List<? extends ProtocolBinding<? extends Object>> protocols) {
        PrivKey privKey = RsaKt.generateRsaKeyPair(2048).getFirst();
        PeerId peerId = PeerId.fromPubKey(privKey.publicKey());
        Multiaddr advertisedAddr = Multiaddr.fromString("/ip4/127.0.0.1/tcp/" + listenPort).withP2P(peerId);
        return buildHost(privKey, List.of("/ip4/127.0.0.1/tcp/" + listenPort), advertisedAddr, protocols);
    }

    public static Host buildHost(PrivKey privKey,
                                 List<String> listenAddrs,
                                 Multiaddr advertisedAddr,
                                 List<? extends ProtocolBinding<? extends Object>> protocols) {
        return BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
            b.getMuxers().add(StreamMuxerProtocol.getMplex());

            b.getProtocols().addAll(protocols);

            IdentifyOuterClass.Identify.Builder identifyBuilder = IdentifyOuterClass.Identify.newBuilder()
                    .setProtocolVersion("ipfs/0.1.0")
                    .setAgentVersion("nabu/v0.1.0")
                    .setPublicKey(ByteArrayExtKt.toProtobuf(privKey.publicKey().bytes()))
                    .addListenAddrs(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()))
                    .setObservedAddr(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()));
            for (ProtocolBinding<?> protocol : protocols) {
                identifyBuilder = identifyBuilder.addAllProtocols(protocol.getProtocolDescriptor().getAnnounceProtocols());
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