package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.protocol.Ping;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.blockstore.*;
import org.peergos.client.RequestSender;
import org.peergos.config.*;
import org.peergos.net.APIHandler;
import org.peergos.net.HttpProxyHandler;
import org.peergos.protocol.autonat.AutonatProtocol;
import org.peergos.protocol.bitswap.Bitswap;
import org.peergos.protocol.bitswap.BitswapEngine;
import org.peergos.protocol.circuit.CircuitHopProtocol;
import org.peergos.protocol.circuit.CircuitStopProtocol;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.HttpUtil;
import org.peergos.util.Logging;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.*;

public class Server {

    private static final Logger LOG = Logger.getLogger(Server.class.getName());

    private Blockstore buildBlockStore(Config config, Path blocksPath) {
        FileBlockstore blocks = new FileBlockstore(blocksPath);
        Blockstore blockStore = null;
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

    public Server(Args args) throws Exception {
        Path ipfsPath = getIPFSPath(args);
        Logging.init(ipfsPath, args.getBoolean("logToConsole", false));
        Config config = readConfig(ipfsPath);
        LOG.info("Starting Nabu version: " + APIService.CURRENT_VERSION);

        Path blocksPath = ipfsPath.resolve("blocks");
        File blocksDirectory = blocksPath.toFile();
        if (!blocksDirectory.exists()) {
            if (!blocksDirectory.mkdir()) {
                throw new IllegalStateException("Unable to make blocks directory");
            }
        } else if (blocksDirectory.isFile()) {
            throw new IllegalStateException("Unable to create blocks directory");
        }
        ProvidingBlockstore blockStore = new ProvidingBlockstore(buildBlockStore(config, blocksPath));

        List<MultiAddress> swarmAddresses = config.addresses.getSwarmAddresses();
        int hostPort = swarmAddresses.get(0).getPort();
        HostBuilder builder = new HostBuilder().setIdentity(config.identity.privKeyProtobuf).listenLocalhost(hostPort);
        if (! builder.getPeerId().equals(config.identity.peerId)) {
            throw new IllegalStateException("PeerId invalid");
        }
        Multihash ourPeerId = Multihash.deserialize(builder.getPeerId().getBytes());

        Path datastorePath = ipfsPath.resolve("datastore").resolve("h2.datastore");
        DatabaseRecordStore records = new DatabaseRecordStore(datastorePath.toString());
        ProviderStore providers = new RamProviderStore();
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
        builder = builder.addProtocols(List.of(
                new Ping(),
                new AutonatProtocol.Binding(),
                new CircuitHopProtocol.Binding(relayManager, stop),
                new Bitswap(new BitswapEngine(blockStore, authoriser)),
                dht,
                p2pHttpBinding));

        Host node = builder.build();
        node.start().join();
        LOG.info("Node started and listening on " + node.listenAddresses());
        LOG.info("Starting bootstrap process");
        int connections = dht.bootstrapRoutingTable(node, config.bootstrap.getBootstrapAddresses(), addr -> !addr.contains("/wss/"));
        if (connections == 0)
            throw new IllegalStateException("No connected peers!");
        dht.bootstrap(node);

        PeriodicBlockProvider blockProvider = new PeriodicBlockProvider(22 * 3600_000L,
                () -> blockStore.refs().join().stream(), node, dht, blockStore.toPublish);
        blockProvider.start();

        String apiAddressArg = "Addresses.API";
        MultiAddress apiAddress = args.hasArg(apiAddressArg) ? new MultiAddress(args.getArg(apiAddressArg)) :  config.addresses.apiAddress;
        InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

        int maxConnectionQueue = 500;
        int handlerThreads = 50;
        LOG.info("Starting RPC API server at " + apiAddress.getHost() + ":" + localAPIAddress.getPort());
        HttpServer apiServer = HttpServer.create(localAPIAddress, maxConnectionQueue);

        APIService service = new APIService(blockStore, new BitswapBlockService(node, builder.getBitswap().get()), dht);
        apiServer.createContext(APIService.API_URL, new APIHandler(service, node));
        apiServer.createContext(HttpProxyService.API_URL, new HttpProxyHandler(new HttpProxyService(node, p2pHttpBinding, dht)));
        apiServer.setExecutor(Executors.newFixedThreadPool(handlerThreads));
        apiServer.start();

        Thread shutdownHook = new Thread(() -> {
            LOG.info("Stopping server...");
            try {
                node.stop().get();
                apiServer.stop(3); //wait max 3 seconds
                records.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private Path getIPFSPath(Args args) {
        Optional<String> ipfsPath = args.getOptionalArg("IPFS_PATH");
        if (ipfsPath.isEmpty()) {
            String home = args.getArg("HOME");
            return Path.of(home, ".ipfs");
        }
        return Path.of(ipfsPath.get());
    }

    private Config readConfig(Path configPath) throws IOException {
        Path configFilePath = configPath.resolve("config");
        File configFile = configFilePath.toFile();
        if (!configFile.exists()) {
            LOG.info("Unable to find config file. Creating default config");
            Config config = new Config();
            Files.write(configFilePath, config.toString().getBytes(), StandardOpenOption.CREATE);
            return config;
        }
        return Config.build(Files.readString(configFilePath));
    }

    public static void main(String[] args) {
        try {
            new Server(Args.parse(args));
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "SHUTDOWN", e);
        }
    }
}