package org.peergos;

import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.dsl.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multiformats.Protocol;
import io.libp2p.core.multistream.*;
import io.libp2p.core.mux.*;
import io.libp2p.crypto.keys.*;
import io.libp2p.protocol.*;
import io.libp2p.protocol.circuit.CircuitHopProtocol;
import io.libp2p.protocol.circuit.CircuitStopProtocol;
import io.libp2p.protocol.circuit.RelayTransport;
import org.peergos.protocol.dcutr.*;
import io.libp2p.security.noise.*;
import io.libp2p.transport.quic.QuicTransport;
import io.libp2p.transport.tcp.*;
import io.libp2p.core.crypto.KeyKt;
import org.peergos.blockstore.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.circuit.*;
import org.peergos.protocol.dht.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class HostBuilder {
    private PrivKey privKey;
    private PeerId peerId;
    private List<String> listenAddrs = new ArrayList<>();
    private List<ProtocolBinding> protocols = new ArrayList<>();
    private List<StreamMuxerProtocol> muxers = new ArrayList<>();
    private final AddressBook addrs;
    private final ReachabilityManager reachability = new ReachabilityManager();
    private CircuitHopProtocol.Binding relayHop;
    private CircuitStopProtocol.Binding relayStop;
    private Function<Host, List<RelayTransport.CandidateRelay>> relayCandidates;
    private DcutrProtocol.Binding dcutr;

    public HostBuilder(AddressBook addrs) {
        this.addrs = addrs;
    }

    public ReachabilityManager getReachability() {
        return reachability;
    }

    /**
     * Enable circuit-relay-v2 using the upstream jvm-libp2p implementation: register the relay hop and
     * stop protocols (relay-server role, gated so we only relay when publicly reachable) and add the
     * relay transport so we can dial and listen on {@code /p2p-circuit} addresses.
     *
     * @param candidateRelays source of relays to auto-reserve on (e.g. discovered via the DHT)
     */
    public HostBuilder enableRelay(Function<Host, List<RelayTransport.CandidateRelay>> candidateRelays) {
        CircuitHopProtocol.RelayManager manager =
                GatingRelayManager.reachabilityGated(privKey, peerId, 5, reachability);
        this.relayStop = new CircuitStopProtocol.Binding(new CircuitStopProtocol());
        this.relayHop = new CircuitHopProtocol.Binding(manager, relayStop);
        this.relayCandidates = candidateRelays;
        return this;
    }

    public HostBuilder enableDcutr() {
        return enableDcutr(conn -> {});
    }

    /**
     * Enable DCUtR (direct connection upgrade through relay): once a relayed connection is established,
     * coordinate a hole punch over it to upgrade to a direct connection.
     *
     * @param onUpgraded invoked with the direct connection once a hole punch succeeds
     */
    public HostBuilder enableDcutr(Consumer<Connection> onUpgraded) {
        this.dcutr = new DcutrProtocol.Binding(
                () -> reachability.getConfirmedPublicAddresses(),
                onUpgraded);
        return this;
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
        return Optional.ofNullable(relayHop);
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
        return create(listenPort, providers, records, blocks, authoriser, false, Optional.empty());
    }

    public static HostBuilder create(int listenPort,
                                     ProviderStore providers,
                                     RecordStore records,
                                     Blockstore blocks,
                                     BlockRequestAuthoriser authoriser,
                                     boolean blockAggressivePeers,
                                     Optional<byte[]> privKey) {
        List<MultiAddress> swarmAddresses = List.of(
                new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort),
                new MultiAddress("/ip4/0.0.0.0/udp/" + listenPort + "/quic-v1")
        );
        HostBuilder builder = new HostBuilder(new RamAddressBook())
                .listen(swarmAddresses);
        builder = privKey.isPresent() ?
                builder.setIdentity(privKey.get()) :
                builder.generateIdentity();
        Multihash ourPeerId = Multihash.deserialize(builder.peerId.getBytes());
        boolean quicEnabled = swarmAddresses.stream()
                .anyMatch(a -> Multiaddr.fromString(a.toString()).has(Protocol.QUICV1));
        boolean tcpEnabled = swarmAddresses.stream()
                .anyMatch(a -> Multiaddr.fromString(a.toString()).has(Protocol.TCP));
        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records, Optional.of(blocks)), false, quicEnabled, tcpEnabled);
        return builder.addProtocols(List.of(
                        new Ping(),
                        new AutonatProtocol.Binding(),
                        new Bitswap(new BitswapEngine(blocks, authoriser, Bitswap.MAX_MESSAGE_SIZE, blockAggressivePeers)),
                        dht))
                .enableRelay(Relay.dhtRelaySource(dht))
                .enableDcutr();
    }

    public static Host build(int listenPort,
                             List<ProtocolBinding> protocols) {
        return new HostBuilder(new RamAddressBook())
                .generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort)))
                .addProtocols(protocols)
                .build();
    }

    public Host build() {
        if (muxers.isEmpty())
            muxers.addAll(List.of(StreamMuxerProtocol.getYamux(), StreamMuxerProtocol.getMplex()));
        return build(privKey, listenAddrs, protocols, muxers, addrs, reachability, relayHop, relayStop, relayCandidates, dcutr);
    }

    public static Host build(PrivKey privKey,
                             List<String> listenAddrs,
                             List<ProtocolBinding> protocols,
                             List<StreamMuxerProtocol> muxers,
                             AddressBook addrs) {
        return build(privKey, listenAddrs, protocols, muxers, addrs, new ReachabilityManager(), null, null, null, null);
    }

    public static Host build(PrivKey privKey,
                             List<String> listenAddrs,
                             List<ProtocolBinding> protocols,
                             List<StreamMuxerProtocol> muxers,
                             AddressBook addrs,
                             ReachabilityManager reachability) {
        return build(privKey, listenAddrs, protocols, muxers, addrs, reachability, null, null, null, null);
    }

    public static Host build(PrivKey privKey,
                             List<String> listenAddrs,
                             List<ProtocolBinding> protocols,
                             List<StreamMuxerProtocol> muxers,
                             AddressBook addrs,
                             ReachabilityManager reachability,
                             CircuitHopProtocol.Binding relayHop,
                             CircuitStopProtocol.Binding relayStop,
                             Function<Host, List<RelayTransport.CandidateRelay>> relayCandidates,
                             DcutrProtocol.Binding dcutr) {
        Host host = BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            List<Multiaddr> toListen = listenAddrs.stream().map(Multiaddr::new).collect(Collectors.toList());
            if (toListen.stream().anyMatch(a -> a.has(Protocol.QUICV1)))
                b.getSecureTransports().add(QuicTransport::ECDSA);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
//            b.getSecureChannels().add(TlsSecureChannel::new);

            b.getMuxers().addAll(muxers);
            b.getAddressBook().setImpl(addrs);
            // Uncomment to add mux debug logging
//            b.getDebug().getMuxFramesHandler().addLogger(LogLevel.INFO, "MUX");

            if (relayHop != null) {
                // Circuit relay v2 (upstream jvm-libp2p): relay-server hop+stop protocols and the
                // relay transport that dials/listens on /p2p-circuit addresses.
                b.getProtocols().add(relayHop);
                b.getProtocols().add(relayStop);
                ScheduledExecutorService relayRunner = Executors.newScheduledThreadPool(1, r -> {
                    Thread t = new Thread(r, "relay-transport");
                    t.setDaemon(true);
                    return t;
                });
                b.getTransports().add(u -> new RelayTransport(relayHop, relayStop, u, relayCandidates, relayRunner));
            }

            if (dcutr != null) {
                b.getProtocols().add(dcutr);
                // The inbound peer of a relayed connection initiates the DCUtR hole punch over it
                b.getConnectionHandlers().add(connection -> {
                    if (connection.isInitiator())
                        return;
                    Multiaddr remote = connection.remoteAddress();
                    if (remote == null || ! remote.toString().contains("p2p-circuit"))
                        return;
                    try {
                        // opening the stream drives the initiator flow (see DcutrProtocol.onStartInitiator)
                        connection.muxerSession().createStream(dcutr);
                    } catch (Exception e) {}
                });
            }

            for (ProtocolBinding<?> protocol : protocols) {
                b.getProtocols().add(protocol);
                if (protocol instanceof AddressBookConsumer)
                    ((AddressBookConsumer) protocol).setAddressBook(addrs);
                if (protocol instanceof ConnectionHandler)
                    b.getConnectionHandlers().add((ConnectionHandler) protocol);
            }

            Optional<Kademlia> wan = protocols.stream()
                    .filter(p -> p instanceof Kademlia && p.getProtocolDescriptor().getAnnounceProtocols().contains("/ipfs/kad/1.0.0"))
                    .map(p -> (Kademlia) p)
                    .findFirst();
            // Send an identify req on all new incoming connections
            b.getConnectionHandlers().add(connection -> {
                PeerId remotePeer = connection.secureSession().getRemoteId();
                Multiaddr remote = connection.remoteAddress().withP2P(remotePeer);
                addrs.addAddrs(remotePeer, 0, remote);
                if (connection.isInitiator())
                    return;
                addrs.getAddrs(remotePeer).thenAccept(existing -> {
                    if (! existing.isEmpty())
                        return;
                    StreamPromise<IdentifyController> stream = connection.muxerSession()
                            .createStream(new IdentifyBinding(new IdentifyProtocol()));
                    stream.getController()
                            .thenCompose(IdentifyController::id)
                            .thenAccept(remoteId -> {
                                Multiaddr[] remoteAddrs = remoteId.getListenAddrsList()
                                        .stream()
                                        .map(bytes -> Multiaddr.deserialize(bytes.toByteArray()))
                                        .toArray(Multiaddr[]::new);

                                addrs.addAddrs(remotePeer, 0, remoteAddrs);
                                // Record the external address the remote observed us connecting from
                                if (remoteId.hasObservedAddr()) {
                                    try {
                                        Multiaddr observed = Multiaddr.deserialize(remoteId.getObservedAddr().toByteArray());
                                        reachability.observeAddress(observed, Multihash.deserialize(remotePeer.getBytes()));
                                    } catch (Exception e) {}
                                }
                                List<String> protocolIds = remoteId.getProtocolsList().stream().collect(Collectors.toList());
                                if (protocolIds.contains(Kademlia.WAN_DHT_ID) && wan.isPresent()) {
                                    // add to kademlia routing table iffi
                                    // 1) we haven't already dialled them
                                    // 2) they accept a new kademlia stream
                                    if (existing.isEmpty())
                                        connection.muxerSession().createStream(wan.get());
                                }
                            });
                });
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
        if (dcutr != null)
            dcutr.setHost(host);
        if (relayHop != null) {
            // The relay transport (and, through it, the hop protocol) needs a reference to the host
            host.getNetwork().getTransports().stream()
                    .filter(t -> t instanceof RelayTransport)
                    .map(t -> (RelayTransport) t)
                    .findFirst()
                    .ifPresent(rt -> {
                        rt.setHost(host);
                        // AutoRelay: reserve on relays (discovered via the candidate source) whenever
                        // we are not publicly reachable. The transport handles reservation, renewal and
                        // reporting our relayed addresses in listenAddresses() for DHT announcement.
                        if (relayCandidates != null) {
                            ExecutorService autoRelayRunner = Executors.newSingleThreadExecutor(r -> {
                                Thread t = new Thread(r, "auto-relay");
                                t.setDaemon(true);
                                return t;
                            });
                            reachability.addListener(state -> {
                                if (state == ReachabilityManager.Reachability.PRIVATE) {
                                    rt.setRelayCount(2);
                                    autoRelayRunner.submit(rt::ensureEnoughCurrentRelays);
                                } else if (state == ReachabilityManager.Reachability.PUBLIC) {
                                    rt.setRelayCount(0);
                                }
                            });
                        }
                    });
        }
        // If we speak AutoNAT, run the client that discovers our own reachability
        protocols.stream()
                .filter(p -> p instanceof AutonatProtocol.Binding)
                .map(p -> (AutonatProtocol.Binding) p)
                .findFirst()
                .ifPresent(autonat -> {
                    Multihash ourId = Multihash.deserialize(host.getPeerId().getBytes());
                    Supplier<List<Multiaddr>> candidates = () -> {
                        List<Multiaddr> candidateAddrs = new ArrayList<>(reachability.getAllObservedAddresses());
                        for (Multiaddr a : host.listenAddresses())
                            if (PeerAddresses.isPublic(a, false))
                                candidateAddrs.add(a);
                        return candidateAddrs.stream().distinct().collect(Collectors.toList());
                    };
                    AutoNatClient client = new AutoNatClient(host, ourId, reachability, autonat, candidates);
                    host.addConnectionHandler(client::onConnection);
                    client.start();
                });
        return host;
    }
}
