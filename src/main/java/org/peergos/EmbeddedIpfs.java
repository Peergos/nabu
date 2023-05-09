package org.peergos;

import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import io.libp2p.protocol.*;
import io.netty.buffer.*;
import io.netty.handler.codec.http.*;
import org.peergos.blockstore.*;
import org.peergos.client.*;
import org.peergos.config.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.circuit.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;
import org.peergos.util.HttpUtil;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class EmbeddedIpfs {
    private static final Logger LOG = Logger.getLogger(EmbeddedIpfs.class.getName());

    public final Host node;
    public final ProvidingBlockstore blockstore;
    public final DatabaseRecordStore records;

    public final Kademlia dht;
    public final Bitswap bitswap;
    public final HttpProtocol.Binding p2pHttp;
    private final List<MultiAddress> bootstrap;

    public EmbeddedIpfs(Host node,
                        ProvidingBlockstore blockstore,
                        DatabaseRecordStore records,
                        Kademlia dht,
                        Bitswap bitswap,
                        HttpProtocol.Binding p2pHttp,
                        List<MultiAddress> bootstrap) {
        this.node = node;
        this.blockstore = blockstore;
        this.records = records;
        this.dht = dht;
        this.bitswap = bitswap;
        this.p2pHttp = p2pHttp;
        this.bootstrap = bootstrap;
    }

    public void start() {
        node.start().join();
        LOG.info("Node started and listening on " + node.listenAddresses());
        LOG.info("Starting bootstrap process");
        int connections = dht.bootstrapRoutingTable(node, bootstrap, addr -> !addr.contains("/wss/"));
        if (connections == 0)
            throw new IllegalStateException("No connected peers!");
        dht.bootstrap(node);

        PeriodicBlockProvider blockProvider = new PeriodicBlockProvider(22 * 3600_000L,
                () -> blockstore.refs().join().stream(), node, dht, blockstore.toPublish);
        blockProvider.start();
    }

    public CompletableFuture<Void> stop() throws Exception {
        records.close();
        return node.stop();
    }

    private static Blockstore buildBlockStore(Config config, Path ipfsPath) {
        Path blocksPath = ipfsPath.resolve("blocks");
        File blocksDirectory = blocksPath.toFile();
        if (!blocksDirectory.exists()) {
            if (!blocksDirectory.mkdir()) {
                throw new IllegalStateException("Unable to make blocks directory");
            }
        } else if (blocksDirectory.isFile()) {
            throw new IllegalStateException("Unable to create blocks directory");
        }
        FileBlockstore blocks = new FileBlockstore(blocksPath);
        Blockstore blockStore;
        if (config.datastore.filter.type == FilterType.BLOOM) {
            blockStore = FilteredBlockstore.bloomBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if(config.datastore.filter.type == FilterType.INFINI) {
            blockStore = FilteredBlockstore.infiniBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if(config.datastore.filter.type == FilterType.NONE) {
            blockStore = blocks;
        } else {
            throw new IllegalStateException("Unhandled filter type: " + config.datastore.filter.type);
        }
        return config.datastore.allowedCodecs.codecs.isEmpty() ?
                blockStore : new TypeLimitedBlockstore(blockStore, config.datastore.allowedCodecs.codecs);
    }

    public static EmbeddedIpfs build(Config config, Path ipfsPath) {
        ProvidingBlockstore blockstore = new ProvidingBlockstore(buildBlockStore(config, ipfsPath));
        Path datastorePath = ipfsPath.resolve("datastore").resolve("h2.datastore");
        DatabaseRecordStore records = new DatabaseRecordStore(datastorePath.toString());
        ProviderStore providers = new RamProviderStore();

        List<MultiAddress> swarmAddresses = config.addresses.getSwarmAddresses();
        int hostPort = swarmAddresses.get(0).getPort();
        HostBuilder builder = new HostBuilder().setIdentity(config.identity.privKeyProtobuf).listenLocalhost(hostPort);
        if (! builder.getPeerId().equals(config.identity.peerId)) {
            throw new IllegalStateException("PeerId invalid");
        }
        Multihash ourPeerId = Multihash.deserialize(builder.getPeerId().getBytes());

        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records), false);
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.getPrivateKey(), ourPeerId, 5);
        BlockRequestAuthoriser authoriser = (c, b, p, a) -> CompletableFuture.completedFuture(true);
        HttpProtocol.Binding p2pHttpBinding = new HttpProtocol.Binding((s, req, h) -> {
            if (config.addresses.proxyTargetAddress.isPresent()) {
                try {
                    FullHttpResponse reply = RequestSender.proxy(config.addresses.proxyTargetAddress.get(), (FullHttpRequest) req);
                    h.accept(reply.retain());
                } catch (IOException ioe) {
                    FullHttpResponse exceptionReply = HttpUtil.replyError(ioe);
                    h.accept(exceptionReply.retain());
                }
            } else {
                FullHttpResponse emptyReply = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.buffer(0));
                emptyReply.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
                h.accept(emptyReply.retain());
            }
        });
        Bitswap bitswap = new Bitswap(new BitswapEngine(blockstore, authoriser));
        builder = builder.addProtocols(List.of(
                new Ping(),
                new AutonatProtocol.Binding(),
                new CircuitHopProtocol.Binding(relayManager, stop),
                bitswap,
                dht,
                p2pHttpBinding));

        Host node = builder.build();

        return new EmbeddedIpfs(node, blockstore, records, dht, bitswap, p2pHttpBinding, config.bootstrap.getBootstrapAddresses());
    }
}
