package org.peergos;

import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.discovery.*;
import io.libp2p.protocol.*;
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

public class EmbeddedIpfs {
    private static final Logger LOG = Logging.LOG();

    public final Host node;
    public final Blockstore blockstore;
    public final BlockService blocks;
    public final RecordStore records;

    public final Kademlia dht;
    public final Bitswap bitswap;
    public final Optional<HttpProtocol.Binding> p2pHttp;
    private final List<MultiAddress> bootstrap;
    private final Optional<PeriodicBlockProvider> blockProvider;
    private final List<MultiAddress> announce;
    private final List<MDnsDiscovery> mdns = new ArrayList<>();

    public EmbeddedIpfs(Host node,
                        Blockstore blockstore,
                        RecordStore records,
                        Kademlia dht,
                        Bitswap bitswap,
                        Optional<HttpProtocol.Binding> p2pHttp,
                        List<MultiAddress> bootstrap,
                        Optional<BlockingDeque<Cid>> newBlockProvider,
                        List<MultiAddress> announce) {
        this.node = node;
        this.blockstore = blockstore;
        this.records = records;
        this.dht = dht;
        this.bitswap = bitswap;
        this.p2pHttp = p2pHttp;
        this.bootstrap = bootstrap;
        this.blocks = new BitswapBlockService(node, bitswap, dht);
        this.blockProvider = newBlockProvider.map(q -> new PeriodicBlockProvider(22 * 3600_000L,
                () -> blockstore.refs(false).join().stream(), node, dht, q));
        this.announce = announce;
    }

    public int maxBlockSize() {
        return bitswap.maxBlockSize();
    }

    public List<HashedBlock> getBlocks(List<Want> wants, Set<PeerId> peers, boolean addToLocal) {
        List<HashedBlock> blocksFound = new ArrayList<>();

        List<Want> local = new ArrayList<>();
        List<Want> remote = new ArrayList<>();

        for (Want w : wants) {
            if (blockstore.has(w.cid).join())
                local.add(w);
            else
                remote.add(w);
        }
        local.stream()
                .forEach(w -> {
                    try {
                        Optional<byte[]> block = blockstore.get(w.cid).join();
                        block.ifPresent(b -> blocksFound.add(new HashedBlock(w.cid, b)));
                        if (block.isEmpty())
                            remote.add(w);
                    } catch (Exception e) {
                        remote.add(w);
                    }
                });
        if (remote.isEmpty())
            return blocksFound;
        return java.util.stream.Stream.concat(
                        blocksFound.stream(),
                        blocks.get(remote, peers, addToLocal).stream())
                .collect(Collectors.toList());
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

    public void start() {
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

        blockProvider.ifPresent(PeriodicBlockProvider::start);
    }

    public CompletableFuture<Void> stop() throws Exception {
        if (records != null) {
            records.close();
        }
        blockProvider.ifPresent(PeriodicBlockProvider::stop);
        dht.stopBootstrapThread();
        for (MDnsDiscovery m : mdns) {
            m.stop();
        }
        return node != null ? node.stop() : CompletableFuture.completedFuture(null);
    }

    public static BlockMetadataStore buildBlockMetadata(Args a) {
        try {
            //see https://www.h2database.com/html/features.html#compatibility for the extra params to support
            // compatibility mode. This is required for 'ON CONFLICT DO NOTHING aka INSERT OR IGNORE INTO'
            Path metadataPath = a.fromIPFSDir("nabu-block-metadata-sql-file", "nabu-blockmetadata.sql");
            java.sql.Connection h2Instance = DriverManager.getConnection("jdbc:h2:" +
                    metadataPath.toAbsolutePath() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH");
            Connection instance = new UncloseableConnection(h2Instance);
            instance.setAutoCommit(true);
            return new JdbcBlockMetadataStore(() -> instance, new H2BlockMetadataCommands());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public static Blockstore buildBlockStore(Config config, Path ipfsPath, BlockMetadataStore meta, boolean updateMetadb) {
        Blockstore withMetadb;
        if (config.datastore.blockMount.prefix.equals("flatfs.datastore")) {
            CachingBlockMetadataStore cachedBlocks = new CachingBlockMetadataStore(new FileBlockstore(ipfsPath), meta);
            if (updateMetadb)
                cachedBlocks.updateMetadataStoreIfEmpty();
            withMetadb = cachedBlocks;
        } else if (config.datastore.blockMount.prefix.equals("s3.datastore")) {
            S3Blockstore s3blocks = new S3Blockstore(config.datastore.blockMount.getParams(), meta);
            if (updateMetadb)
                s3blocks.updateMetadataStoreIfEmpty();
            withMetadb = s3blocks;
        } else {
            throw new IllegalStateException("Unrecognized datastore prefix: " + config.datastore.blockMount.prefix);
        }
        return typeLimited(filteredBlockStore(withMetadb, config), config);
    }

    public static Blockstore typeLimited(Blockstore blocks, Config config) {
        return config.datastore.allowedCodecs.codecs.isEmpty() ?
                blocks :
                new TypeLimitedBlockstore(blocks, config.datastore.allowedCodecs.codecs);
    }

    public static Blockstore filteredBlockStore(Blockstore blocks, Config config) {
        if (config.datastore.filter.type == FilterType.BLOOM) {
            return FilteredBlockstore.bloomBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if(config.datastore.filter.type == FilterType.INFINI) {
            return FilteredBlockstore.infiniBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if(config.datastore.filter.type == FilterType.NONE) {
            return blocks;
        } else {
            throw new IllegalStateException("Unhandled filter type: " + config.datastore.filter.type);
        }
    }

    public static EmbeddedIpfs build(RecordStore records,
                                     Blockstore blocks,
                                     boolean provideBlocks,
                                     List<MultiAddress> swarmAddresses,
                                     List<MultiAddress> bootstrap,
                                     IdentitySection identity,
                                     BlockRequestAuthoriser authoriser,
                                     Optional<HttpProtocol.HttpRequestProcessor> handler) {
        return build(records, blocks, provideBlocks, swarmAddresses, bootstrap, identity, Collections.emptyList(),
                authoriser, handler, Optional.empty(), Optional.empty());
    }

    public static EmbeddedIpfs build(RecordStore records,
                                     Blockstore blocks,
                                     boolean provideBlocks,
                                     List<MultiAddress> swarmAddresses,
                                     List<MultiAddress> bootstrap,
                                     IdentitySection identity,
                                     List<MultiAddress> announce,
                                     BlockRequestAuthoriser authoriser,
                                     Optional<HttpProtocol.HttpRequestProcessor> handler,
                                     Optional<String> bitswapProtocolId,
                                     Optional<Integer> maxBitswapMsgSize) {
        Blockstore blockstore = provideBlocks ?
                new ProvidingBlockstore(blocks) :
                blocks;
        ProviderStore providers = new RamProviderStore(10_000);

        HostBuilder builder = new HostBuilder().setIdentity(identity.privKeyProtobuf).listen(swarmAddresses);
        if (! builder.getPeerId().equals(identity.peerId)) {
            throw new IllegalStateException("PeerId invalid");
        }
        Multihash ourPeerId = Multihash.deserialize(builder.getPeerId().getBytes());

        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records, blockstore), false);
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.getPrivateKey(), ourPeerId, 5);
        Bitswap bitswap = new Bitswap(bitswapProtocolId.orElse(Bitswap.PROTOCOL_ID),
                new BitswapEngine(blockstore, authoriser, maxBitswapMsgSize.orElse(Bitswap.MAX_MESSAGE_SIZE), false));
        Optional<HttpProtocol.Binding> httpHandler = handler.map(HttpProtocol.Binding::new);

        List<ProtocolBinding> protocols = new ArrayList<>();
        protocols.add(new Ping());
        protocols.add(new AutonatProtocol.Binding());
        protocols.add(new CircuitHopProtocol.Binding(relayManager, stop));
        protocols.add(bitswap);
        protocols.add(dht);
        httpHandler.ifPresent(protocols::add);

        Host node = builder.addProtocols(protocols).build();

        Optional<BlockingDeque<Cid>> newBlockProvider = provideBlocks ?
                Optional.of(((ProvidingBlockstore)blockstore).toPublish) :
                Optional.empty();
        return new EmbeddedIpfs(node, blockstore, records, dht, bitswap, httpHandler, bootstrap, newBlockProvider, announce);
    }

    public static Multiaddr[] getAddresses(Host node, Kademlia dht, Multihash targetNodeId) throws ConnectionException {
        AddressBook addressBook = node.getAddressBook();
        Multihash targetPeerId = targetNodeId.bareMultihash();
        PeerId peerId = PeerId.fromBase58(targetPeerId.toBase58());
        Optional<Multiaddr> targetAddressesOpt = addressBook.get(peerId).join().stream().findFirst();
        Multiaddr[] allAddresses = null;
        if (targetAddressesOpt.isEmpty()) {
            List<PeerAddresses> closestPeers = dht.findClosestPeers(targetPeerId, 1, node);
            Optional<PeerAddresses> matching = closestPeers.stream().filter(p -> p.peerId.equals(targetPeerId)).findFirst();
            if (matching.isEmpty()) {
                throw new ConnectionException("Target not found: " + targetPeerId);
            }
            PeerAddresses peer = matching.get();
            allAddresses = peer.addresses.stream().map(a -> Multiaddr.fromString(a.toString())).toArray(Multiaddr[]::new);
        }
        return targetAddressesOpt.isPresent() ?
                Arrays.asList(targetAddressesOpt.get()).toArray(Multiaddr[]::new)
                : allAddresses;
    }
}
