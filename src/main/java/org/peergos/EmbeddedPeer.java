package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.crypto.PubKey;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.ProtocolBinding;
import io.libp2p.discovery.MDnsDiscovery;
import io.libp2p.protocol.Ping;
import org.peergos.blockstore.*;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;
import org.peergos.blockstore.metadatadb.JdbcBlockMetadataStore;
import org.peergos.blockstore.metadatadb.CachingBlockMetadataStore;
import org.peergos.blockstore.metadatadb.sql.H2BlockMetadataCommands;
import org.peergos.blockstore.metadatadb.sql.UncloseableConnection;
import org.peergos.blockstore.s3.S3Blockstore;
import org.peergos.cbor.Cborable;
import org.peergos.config.*;
import org.peergos.net.ConnectionException;
import org.peergos.protocol.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.circuit.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;
import org.peergos.protocol.ipns.*;
import org.peergos.util.Logging;

import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.stream.*;

public class EmbeddedPeer {
    private static final Logger LOG = Logging.LOG();

    public final Host node;
    public final RecordStore records;

    public final Kademlia dht;
    public final Optional<HttpProtocol.Binding> p2pHttp;
    private final List<MultiAddress> bootstrap;
    private final List<MultiAddress> announce;
    private final List<MDnsDiscovery> mdns = new ArrayList<>();

    public EmbeddedPeer(Host node,
                        RecordStore records,
                        Kademlia dht,
                        Optional<HttpProtocol.Binding> p2pHttp,
                        List<MultiAddress> bootstrap,
                        List<MultiAddress> announce) {
        this.node = node;
        this.records = records;
        this.dht = dht;
        this.p2pHttp = p2pHttp;
        this.bootstrap = bootstrap;
        this.announce = announce;
    }

    public CompletableFuture<Integer> publishValue(PrivKey priv, byte[] value, Optional<String> extraDataKeySuffix, Optional<Cborable> extraData, long sequence, int hoursTtl) {
        Multihash pub = Multihash.deserialize(PeerId.fromPubKey(priv.publicKey()).getBytes());
        LocalDateTime expiry = LocalDateTime.now().plusHours(hoursTtl);
        long ttlNanos = hoursTtl * 3600_000_000_000L;
        byte[] signedRecord = IPNS.createSignedRecord(value, expiry, sequence, ttlNanos, extraDataKeySuffix, extraData, priv);
        return dht.publishValue(pub, signedRecord, node);
    }

    public CompletableFuture<Integer> publishPresignedRecord(Multihash pub, byte[] presignedRecord) {
        return dht.publishValue(pub, presignedRecord, node);
    }

    public CompletableFuture<byte[]> resolveValue(PubKey pub, int minResults) {
        Multihash publisher = Multihash.deserialize(PeerId.fromPubKey(pub).getBytes());
        List<IpnsRecord> candidates = dht.resolveValue(publisher, minResults, node);
        List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        if (records.isEmpty())
            return CompletableFuture.failedFuture(new IllegalStateException("Couldn't resolve IPNS value for " + pub));
        return CompletableFuture.completedFuture(records.get(records.size() - 1).value);
    }

    public List<IpnsRecord> resolveRecords(Multihash publisher, int minResults) {
        List<IpnsRecord> candidates = dht.resolveValue(publisher, minResults, node);
        return candidates.stream().sorted().collect(Collectors.toList());
    }

    public void start(boolean asyncBootstrap) {
        LOG.info("Starting IPFS...");
        Thread shutdownHook = new Thread(() -> {
            LOG.info("Stopping Ipfs server...");
            try {
                this.stop().join();
            } catch (Exception ex) {
                LOG.info(ex.getMessage());
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        node.start().join();
        IdentifyBuilder.addIdentifyProtocol(node, announce.stream()
                .map(MultiAddress::toString)
                .map(Multiaddr::new)
                .collect(Collectors.toList()));
        LOG.info("Node started and listening on " + node.listenAddresses());
        LOG.info("Bootstrapping IPFS routing table");
        if (bootstrap.isEmpty())
            LOG.warning("Starting with empty bootstrap list - you will not join the global dht");
        int connections = dht.bootstrapRoutingTable(node, bootstrap, addr -> !addr.contains("/wss/"));
        LOG.info("Bootstrapping IPFS kademlia");
        if (! asyncBootstrap)
            dht.bootstrap(node);
        dht.startBootstrapThread(node);

        LOG.info("MDNS discovery enabled");
        MDnsDiscovery mdns = new MDnsDiscovery(node, "_ipfs-discovery._udp.local.", 60, null);
        this.mdns.add(mdns);
        mdns.addHandler(peerInfo -> {
            PeerId remote = PeerId.fromBase58(peerInfo.getPeerId().toBase58().substring(1)); // Not sure what's wrong with peerInfo, but this works
            if (!remote.equals(node.getPeerId())) {
                LOG.info(node.getPeerId() + " found local peer: " + remote.toBase58() + ", addrs: " + peerInfo.getAddresses());
                Multiaddr[] remoteAddrs = peerInfo.getAddresses().toArray(new Multiaddr[0]);
                KademliaController ctr = dht.dial(node, remote, remoteAddrs).getController().join();
                ctr.closerPeers(node.getPeerId().getBytes()).join();
                node.getAddressBook().addAddrs(remote, 0, remoteAddrs);
            }
            return null;
        });
        mdns.start();
    }

    public CompletableFuture<Void> stop() throws Exception {
        if (records != null) {
            records.close();
        }
        dht.stopBootstrapThread();
        for (MDnsDiscovery m : mdns) {
            m.stop();
        }
        return node != null ? node.stop() : CompletableFuture.completedFuture(null);
    }

    public static EmbeddedPeer build(RecordStore records,
                                     List<MultiAddress> swarmAddresses,
                                     List<MultiAddress> bootstrap,
                                     IdentitySection identity,
                                     List<MultiAddress> announce,
                                     Optional<HttpProtocol.HttpRequestProcessor> handler) {
        ProviderStore providers = new RamProviderStore(10_000);

        HostBuilder builder = new HostBuilder().setIdentity(identity.privKeyProtobuf).listen(swarmAddresses);
        if (! builder.getPeerId().equals(identity.peerId)) {
            throw new IllegalStateException("PeerId invalid");
        }
        Multihash ourPeerId = Multihash.deserialize(builder.getPeerId().getBytes());

        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records, Optional.empty()), false);
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.getPrivateKey(), ourPeerId, 5);
        Optional<HttpProtocol.Binding> httpHandler = handler.map(HttpProtocol.Binding::new);

        List<ProtocolBinding> protocols = new ArrayList<>();
        protocols.add(new Ping());
        protocols.add(new AutonatProtocol.Binding());
        protocols.add(new CircuitHopProtocol.Binding(relayManager, stop));

        protocols.add(dht);
        httpHandler.ifPresent(protocols::add);

        Host node = builder.addProtocols(protocols).build();

        return new EmbeddedPeer(node, records, dht, httpHandler, bootstrap, announce);
    }

    public static Multiaddr[] getAddresses(Host node, Kademlia dht, Multihash targetNodeId) throws ConnectionException {
        AddressBook addressBook = node.getAddressBook();
        Multihash targetPeerId = targetNodeId.bareMultihash();
        PeerId peerId = PeerId.fromBase58(targetPeerId.toBase58());
        Collection<Multiaddr> all = addressBook.get(peerId).join();
        if (! all.isEmpty())
            return all.toArray(Multiaddr[]::new);
        Multiaddr[] allAddresses = null;
        if (all.isEmpty()) {
            List<PeerAddresses> closestPeers = dht.findClosestPeers(targetPeerId, 1, node);
            Optional<PeerAddresses> matching = closestPeers.stream().filter(p -> p.peerId.equals(targetPeerId)).findFirst();
            if (matching.isEmpty()) {
                throw new ConnectionException("Target not found: " + targetPeerId);
            }
            PeerAddresses peer = matching.get();
            allAddresses = peer.addresses.stream().map(a -> Multiaddr.fromString(a.toString())).toArray(Multiaddr[]::new);
            addressBook.setAddrs(peerId, 0, allAddresses);
        }
        return allAddresses;
    }
}
