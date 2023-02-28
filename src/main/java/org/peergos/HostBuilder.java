package org.peergos;

import identify.pb.*;
import io.ipfs.multibase.binary.Base64;
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
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.circuit.*;
import org.peergos.protocol.dht.*;

import java.util.*;

public class HostBuilder {
    private PrivKey privKey;
    private PeerId peerId;
    private List<String> listenAddrs = new ArrayList<>();
    private Multiaddr advertisedAddr;
    private List<ProtocolBinding> protocols = new ArrayList<>();
    private List<StreamMuxerProtocol> muxers = new ArrayList<>();

    public HostBuilder() {
    }

    public PrivKey getPrivateKey() {
        return privKey;
    }

    public PeerId getPeerId() {
        return peerId;
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

    public Optional<CircuitHopProtocol.Binding> getRelayHop() {
        return protocols.stream()
                .filter(p -> p instanceof CircuitHopProtocol.Binding)
                .map(p -> (CircuitHopProtocol.Binding)p)
                .findFirst();
    }

    public HostBuilder addMuxers(List<StreamMuxerProtocol> muxers) {
        this.muxers.addAll(muxers);
        return this;
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

    public HostBuilder setIdentity(String base64PrivKey) {
        byte[] privKey = Base64.decodeBase64(base64PrivKey);
        return setPrivKey(Ed25519Kt.unmarshalEd25519PrivateKey(privKey));
    }



    public HostBuilder setPrivKey(PrivKey privKey) {
        this.privKey = privKey;
        this.peerId = PeerId.fromPubKey(privKey.publicKey());
        return this;
    }

    public static HostBuilder build(int listenPort, ProviderStore providers, RecordStore records, Blockstore blocks) {
        HostBuilder builder = new HostBuilder().generateIdentity().listenLocalhost(listenPort);
        Multihash ourPeerId = Multihash.deserialize(builder.peerId.getBytes());
        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records), false);
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.privKey, ourPeerId, 5);
        return builder.addProtocols(List.of(
                new Ping(),
                new AutonatProtocol.Binding(),
                new CircuitHopProtocol.Binding(relayManager, stop),
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
        if (muxers.isEmpty())
            muxers.addAll(List.of(StreamMuxerProtocol.getYamux(), StreamMuxerProtocol.getMplex()));
        return build(privKey, listenAddrs, advertisedAddr, protocols, muxers);
    }

    public static Host build(PrivKey privKey,
                             List<String> listenAddrs,
                             Multiaddr advertisedAddr,
                             List<ProtocolBinding> protocols,
                             List<StreamMuxerProtocol> muxers) {
        Host host = BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
            b.getMuxers().addAll(muxers);
            b.getAddressBook().setImpl(new RamAddressBook());

            for (ProtocolBinding<?> protocol : protocols) {
                b.getProtocols().add(protocol);
                if (protocol instanceof AddressBookConsumer)
                    ((AddressBookConsumer) protocol).setAddressBook(b.getAddressBook().getImpl());
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
        for (ProtocolBinding protocol : protocols) {
            if (protocol instanceof HostConsumer)
                ((HostConsumer)protocol).setHost(host);
        }
        return host;
    }
}
