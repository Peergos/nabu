package org.peergos;

import identify.pb.*;
import io.ipfs.multihash.Multihash;
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
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;

import java.util.*;

public class HostBuilder {
    private PrivKey privKey;
    private PeerId peerId;
    private List<String> listenAddrs = new ArrayList<>();
    private Multiaddr advertisedAddr;
    private List<ProtocolBinding> protocols = new ArrayList<>();

    public HostBuilder() {
    }

    public Optional<Kademlia> getWanDht() {
        return protocols.stream()
                .filter(p -> p instanceof Kademlia && p.getProtocolDescriptor().getAnnounceProtocols().contains("/ipfs/kad/1.0.0"))
                .map(p -> (Kademlia)p)
                .findFirst();
    }

    public Optional<Bitswap> getBitswap() {
        return protocols.stream()
                .filter(p -> p instanceof Bitswap)
                .map(p -> (Bitswap)p)
                .findFirst();
    }

    public HostBuilder addProtocols(List<ProtocolBinding> protocols) {
        this.protocols.addAll(protocols);
        return this;
    }

    public HostBuilder addProtocol(ProtocolBinding protocols) {
        this.protocols.add(protocols);
        return this;
    }

    public HostBuilder advertiseLocalhost(int listenPort) {
        advertisedAddr = Multiaddr.fromString("/ip4/127.0.0.1/tcp/" + listenPort).withP2P(peerId);
        return this;
    }

    public HostBuilder listenLocalhost(int listenPort) {
        listenAddrs.add("/ip4/127.0.0.1/tcp/" + listenPort);
        return advertiseLocalhost(listenPort);
    }

    public HostBuilder generateIdentity() {
        return setPrivKey(Ed25519Kt.generateEd25519KeyPair().getFirst());
    }

    public HostBuilder setPrivKey(PrivKey privKey) {
        this.privKey = privKey;
        this.peerId = PeerId.fromPubKey(privKey.publicKey());
        return this;
    }

    public static HostBuilder build(int listenPort, ProviderStore providers, RecordStore records, Blockstore blocks) {
        HostBuilder builder = new HostBuilder().generateIdentity().advertiseLocalhost(listenPort);
        Kademlia dht = new Kademlia(new KademliaEngine(Multihash.deserialize(builder.peerId.getBytes()), providers, records), false);
        return builder.addProtocols(List.of(
                new Ping(),
                new Bitswap(new BitswapEngine(blocks)),
                dht));
    }

    public static Host build(int listenPort,
                             List<ProtocolBinding> protocols) {
        return new HostBuilder()
                .generateIdentity()
                .listenLocalhost(listenPort)
                .addProtocols(protocols)
                .build();
    }

    public Host build() {
        return build(privKey, listenAddrs, advertisedAddr, protocols);
    }

    public static Host build(PrivKey privKey,
                             List<String> listenAddrs,
                             Multiaddr advertisedAddr,
                             List<ProtocolBinding> protocols) {
        return BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
            b.getMuxers().add(StreamMuxerProtocol.getMplex());

            for (ProtocolBinding<?> protocol : protocols) {
                b.getProtocols().add(protocol);
                if (protocol instanceof Kademlia)
                    ((Kademlia) protocol).setAddressBook(b.getAddressBook().getImpl());
            }

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
