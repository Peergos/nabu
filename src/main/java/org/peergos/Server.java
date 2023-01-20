package org.peergos;

import bitswap.message.pb.*;
import com.google.protobuf.*;
import identify.pb.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
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

import java.nio.charset.*;
import java.util.*;

public class Server {

    public static void main(String[] args) throws Exception {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        Host node1 = buildHost(4001, bitswap1);
        node1.start().get();
        System.out.println("Node 1 started and listening on " + node1.listenAddresses());

        RamBlockstore blockstore2 = new RamBlockstore();
        Bitswap bitswap2 = new Bitswap(new BitswapEngine(blockstore2));
        Host node2 = buildHost(7001, bitswap2);
        node2.start().get();
        System.out.println("Node 2 started and listening on " + node2.listenAddresses());

        Multiaddr address2 = node2.listenAddresses().get(0);
        pingTest(node1, address2);

        System.out.println("Sending a bitswap message");
        byte[] blockData = "G'day from Java bitswap!".getBytes(StandardCharsets.UTF_8);
        Cid hash = blockstore2.put(blockData, Cid.Codec.Raw).join();
        BitswapController bc1 = bitswap1.dial(node1, address2).getController().join();
        byte[] receivedBlock = bitswap1.getEngine().get(hash).join();

        node1.stop().get();
        node2.stop().get();
    }

    public static void pingTest(Host node1, Multiaddr address2) {
        PingController pinger = new Ping().dial(node1, address2).getController().join();

        System.out.println("Sending 5 ping messages to " + address2);
        for (int i = 0; i < 2; i++) {
            long latency = pinger.ping().join();
            System.out.println("Ping " + i + ", latency " + latency + "ms");
        }
    }

    public static Host buildHost(int listenPort,
                                 Bitswap bitswap) {
        PrivKey privKey = RsaKt.generateRsaKeyPair(2048).getFirst();
        PeerId peerId = PeerId.fromPubKey(privKey.publicKey());
        Multiaddr advertisedAddr = Multiaddr.fromString("/ip4/127.0.0.1/tcp/" + listenPort).withP2P(peerId);
        return buildHost(privKey, List.of("/ip4/127.0.0.1/tcp/" + listenPort), advertisedAddr, bitswap);
    }

    public static Host buildHost(PrivKey privKey,
                                 List<String> listenAddrs,
                                 Multiaddr advertisedAddr,
                                 Bitswap bitswap) {
        return BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
            b.getMuxers().add(StreamMuxerProtocol.getMplex());

            Ping ping = new Ping();
            b.getProtocols().add(ping);
            b.getProtocols().add(bitswap);
            b.getProtocols().add(new Identify(IdentifyOuterClass.Identify.newBuilder()
                    .setProtocolVersion("ipfs/0.1.0")
                    .setAgentVersion("nabu/v0.1.0")
                    .setPublicKey(ByteArrayExtKt.toProtobuf(privKey.publicKey().bytes()))
                    .addListenAddrs(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()))
                    .setObservedAddr(ByteArrayExtKt.toProtobuf(advertisedAddr.serialize()))
                    .addAllProtocols(ping.getProtocolDescriptor().getAnnounceProtocols())
                    .addAllProtocols(bitswap.getProtocolDescriptor().getAnnounceProtocols())
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