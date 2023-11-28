package org.peergos;

import identify.pb.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.crypto.*;
import io.libp2p.core.dsl.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.core.mux.*;
import io.libp2p.core.security.*;
import io.libp2p.core.transport.*;
import io.libp2p.crypto.keys.*;
import io.libp2p.etc.types.*;
import io.libp2p.etc.util.netty.*;
import io.libp2p.multistream.*;
import io.libp2p.protocol.*;
import io.libp2p.security.noise.*;
import io.libp2p.security.tls.*;
import io.libp2p.transport.*;
import io.libp2p.transport.implementation.*;
import io.libp2p.transport.tcp.*;
import io.libp2p.core.crypto.KeyKt;
import kotlin.jvm.functions.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.circuit.*;
import org.peergos.protocol.dht.*;

import java.time.*;
import java.time.temporal.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

public class HostBuilder {
    private PrivKey privKey;
    private PeerId peerId;
    private List<String> listenAddrs = new ArrayList<>();
    private List<ProtocolBinding> protocols = new ArrayList<>();
    private List<StreamMuxerProtocol> muxers = new ArrayList<>();
    private List<Function1<ConnectionUpgrader, Transport>> transports = new ArrayList<>();

    public HostBuilder() {
    }

    public PrivKey getPrivateKey() {
        return privKey;
    }

    public PeerId getPeerId() {
        return peerId;
    }

    public List<ProtocolBinding> getProtocols() {
        return this.protocols;
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

    public HostBuilder listen(List<MultiAddress> listenAddrs) {
        this.listenAddrs.addAll(listenAddrs.stream().map(MultiAddress::toString).collect(Collectors.toList()));
        return this;
    }

    public HostBuilder generateIdentity() {
        return setPrivKey(Ed25519Kt.generateEd25519KeyPair().getFirst());
    }

    public HostBuilder setIdentity(byte[] privKey) {
        return setPrivKey(KeyKt.unmarshalPrivateKey(privKey));
    }



    public HostBuilder setPrivKey(PrivKey privKey) {
        this.privKey = privKey;
        this.peerId = PeerId.fromPubKey(privKey.publicKey());
        return this;
    }

    public static HostBuilder create(int listenPort,
                                     ProviderStore providers,
                                     RecordStore records,
                                     Blockstore blocks,
                                     BlockRequestAuthoriser authoriser) {
        return create(listenPort, providers, records, blocks, authoriser, false);
    }

    private void upgradeAndlimitConnection(Stream s, Multihash peerid, int durationSeconds, long limitBytes) {
        s.pushHandler(new InboundTrafficLimitHandler(limitBytes));
        s.pushHandler(new TotalTimeoutHandler(Duration.of(durationSeconds, ChronoUnit.SECONDS)));
        upgradeConnection(s, peerid);
    }

    private void upgradeConnection(Stream s, Multihash peerid) {
        List<? extends ProtocolBinding<?>> bindings = protocols.stream()
                .map(b -> (ProtocolBinding<?>)b)
                .collect(Collectors.toList());
        List<StreamMuxer> theMuxers = muxers.stream()
                .map(m -> m.createMuxer(new MultistreamProtocolDebugV1(), bindings))
                .collect(Collectors.toList());
        List<SecureChannel> sec = List.of(new NoiseXXSecureChannel(privKey, theMuxers));
        ConnectionUpgrader upgrader = new ConnectionUpgrader(
                new MultistreamProtocolDebugV1(), sec,
                new MultistreamProtocolDebugV1(), theMuxers);
        ConnectionHandler handler = null;//new MultistreamProtocolDebugV1().createMultistream(bindings).toStreamHandler();
        ConnectionBuilder connBuilder = new ConnectionBuilder(null, upgrader, handler,
                false, PeerId.fromBase58(peerid.toBase58()), null);

        s.getConnection().pushHandler(connBuilder);
        connBuilder.getConnectionEstablished().thenApply(c -> c.muxerSession().createStream(bindings));

/*        upgrader.establishSecureChannel(conn).thenCompose(sess -> {
//            conn.setSecureSession(sess);
            if (sess.getEarlyMuxer() != null) {
                return upgrader.establishMuxer(sess.getEarlyMuxer(), conn);
            } else
                return upgrader.establishMuxer(conn);
        });*/
    }

    public HostBuilder enableRelay() {
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding(this::upgradeAndlimitConnection);
        Multihash ourPeerId = Multihash.deserialize(peerId.getBytes());
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(privKey, ourPeerId, 5);
        CircuitHopProtocol.Binding hop = new CircuitHopProtocol.Binding(relayManager, stop);
        addProtocols(List.of(stop, hop));
        transports.add(TcpTransport::new);
        Kademlia kademlia = getWanDht().get();
        transports.add(upg -> new RelayTransport(hop, upg, host -> RelayDiscovery.findRelay(kademlia, host)));
        return this;
    }

    public static HostBuilder create(int listenPort,
                                     ProviderStore providers,
                                     RecordStore records,
                                     Blockstore blocks,
                                     BlockRequestAuthoriser authoriser,
                                     boolean blockAggressivePeers) {
        HostBuilder builder = new HostBuilder()
                .generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort)));
        Multihash ourPeerId = Multihash.deserialize(builder.peerId.getBytes());
        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records, blocks), false);

        return builder.addProtocols(List.of(
                new Ping(),
                new AutonatProtocol.Binding(),
                new Bitswap(new BitswapEngine(blocks, authoriser, Bitswap.MAX_MESSAGE_SIZE, blockAggressivePeers)),
                dht));
    }

    public static Host build(int listenPort,
                             List<ProtocolBinding> protocols) {
        return new HostBuilder()
                .generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort)))
                .addProtocols(protocols)
                .build();
    }

    public Host build() {
        if (muxers.isEmpty())
            muxers.addAll(List.of(StreamMuxerProtocol.getYamux(), StreamMuxerProtocol.getMplex()));
        if (transports.isEmpty())
            transports.add(TcpTransport::new);
        return build(privKey, listenAddrs, transports, protocols, muxers);
    }

    public static Host build(PrivKey privKey,
                             List<String> listenAddrs,
                             List<Function1<ConnectionUpgrader, Transport>> transports,
                             List<ProtocolBinding> protocols,
                             List<StreamMuxerProtocol> muxers) {
        Host host = BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().addAll(transports);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
//            b.getSecureChannels().add(TlsSecureChannel::new);

            b.getMuxers().addAll(muxers);
            RamAddressBook addrs = new RamAddressBook();
            b.getAddressBook().setImpl(addrs);
            // Uncomment to add mux debug logging
//            b.getDebug().getMuxFramesHandler().addLogger(LogLevel.INFO, "MUX");

            for (ProtocolBinding<?> protocol : protocols) {
                b.getProtocols().add(protocol);
                if (protocol instanceof AddressBookConsumer)
                    ((AddressBookConsumer) protocol).setAddressBook(addrs);
                if (protocol instanceof ConnectionHandler)
                    b.getConnectionHandlers().add((ConnectionHandler) protocol);
            }

            // Send an identify req on all new incoming connections
            b.getConnectionHandlers().add(connection -> {
                PeerId remotePeer = connection.secureSession().getRemoteId();
                Multiaddr remote = connection.remoteAddress().withP2P(remotePeer);
                addrs.addAddrs(remotePeer, 0, remote);
                if (connection.isInitiator())
                    return;
                StreamPromise<IdentifyController> stream = connection.muxerSession()
                        .createStream(new IdentifyBinding(new IdentifyProtocol()));
                stream.getController()
                        .thenCompose(IdentifyController::id)
                        .thenApply(remoteId -> addrs.addAddrs(remotePeer, 0, remoteId.getListenAddrsList()
                                .stream()
                                .map(bytes -> Multiaddr.deserialize(bytes.toByteArray()))
                                .toArray(Multiaddr[]::new)));
            });

            for (String listenAddr : listenAddrs) {
                b.getNetwork().listen(listenAddr);
            }

//            b.getConnectionHandlers().add(conn -> System.out.println(conn.localAddress() +
//                    " received connection from " + conn.remoteAddress() +
//                    " on transport " + conn.transport()));
        });
        for (ProtocolBinding protocol : protocols) {
            if (protocol instanceof HostConsumer)
                ((HostConsumer)protocol).setHost(host);
        }
        for (Transport transport : host.getNetwork().getTransports()) {
            if (transport instanceof HostConsumer)
                ((HostConsumer)transport).setHost(host);
        }
        return host;
    }
}
