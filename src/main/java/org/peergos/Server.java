package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.protocol.Ping;
import org.peergos.blockstore.*;
import org.peergos.protocol.autonat.AutonatProtocol;
import org.peergos.protocol.bitswap.Bitswap;
import org.peergos.protocol.bitswap.BitswapEngine;
import org.peergos.protocol.circuit.CircuitHopProtocol;
import org.peergos.protocol.circuit.CircuitStopProtocol;
import org.peergos.protocol.dht.*;
import org.peergos.util.Config;
import org.peergos.util.JSONParser;
import org.peergos.util.Logging;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Server {

    public Server() {
        try {
            String home = System.getenv("HOME");
            Path configPath = Path.of(home, ".ipfs");

            Logging.init(configPath);
            Config config = readConfig(configPath);
            System.out.println("Starting Nabu version: " + APIService.CURRENT_VERSION);
            String specType = config.getProperty("Datastore", "Spec", "type");
            if (!specType.equals("mount")) {
                throw new IllegalStateException("Unable to read mount configuration");
            }
            List<Map> mounts = config.getPropertyList("Datastore", "Spec", "mounts");
            Optional<Map> blockMountOpt = mounts.stream().filter(m -> m.get("mountpoint").equals("/blocks")).findFirst();
            if (blockMountOpt.isEmpty()) {
                throw new IllegalStateException("Unable to fine /blocks mount");
            }
            Config blockConfig = new Config(blockMountOpt.get());
            String blockPath = blockConfig.getProperty("child", "path");
            String blockShardFunc = blockConfig.getProperty("child", "shardFunc");
            String blockType = blockConfig.getProperty("child", "type");
            if (!(blockPath.equals("blocks") && blockShardFunc.equals("/repo/flatfs/shard/v1/next-to-last/2")
                && blockType.equals("flatfs"))) {
                throw new IllegalStateException("Expecting flatfs mount at /blocks");
            }
            Path blocksPath = Path.of(System.getenv("HOME"), ".ipfs", "blocks");
            File blocksDirectory = blocksPath.toFile();
            if (!blocksDirectory.exists()) {
                if (!blocksDirectory.mkdir()) {
                    throw new IllegalStateException("Unable to make blocks directory");
                }
            } else if(blocksDirectory.isFile()) {
                throw new IllegalStateException("Unable to create blocks directory");
            }
            FileBlockstore blocks = new FileBlockstore(blocksPath);
            //todo use DatabaseRecordStore
            RecordStore records = new RamRecordStore();
            ProviderStore providers = new RamProviderStore();
            int hostPort = 6001; // 6001
            //todo get values as required from Config for Addresses, Bootstrap etc
            // ie make sure to enable p2p proxy only if Experimental.P2pHttpProxy == true
            // also get BloomFilterSize from Datastore.BloomFilterSize
            // also get ProxyTarget from Addresses.ProxyTarget
            String privKey = config.getProperty("Identity", "PrivKey");
            HostBuilder builder = new HostBuilder().setIdentity(privKey).listenLocalhost(hostPort);
            Multihash ourPeerId = Multihash.deserialize(builder.getPeerId().getBytes());
            Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records), false);
            CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
            CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.getPrivateKey(), ourPeerId, 5);
            builder = builder.addProtocols(List.of(
                    new Ping(),
                    new AutonatProtocol.Binding(),
                    new CircuitHopProtocol.Binding(relayManager, stop),
                    new Bitswap(new BitswapEngine(blocks)),
                    dht));

            Host node = builder.build();
            node.start().join();
            System.out.println("Node started and listening on " + node.listenAddresses());

//            Multiaddr bootstrapNode = Multiaddr.fromString("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt");
//            KademliaController bootstrap = builder.getWanDht().get().dial(node, bootstrapNode).getController().join();

            APIService localAPI = new APIService();
            MultiAddress apiAddress = new MultiAddress(config.getProperty("Addresses", "API"));
            InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

            int maxConnectionQueue = 500;
            int handlerThreads = 50;
            HttpServer apiServer = localAPI.initAndStart(localAPIAddress, node, blocks, maxConnectionQueue, handlerThreads);

            Thread shutdownHook = new Thread(() -> {
                System.out.println("Stopping server...");
                try {
                    node.stop().get();
                    apiServer.stop(3); //wait max 3 seconds
                } catch (InterruptedException | ExecutionException ex) {
                    ex.printStackTrace();
                }
            });
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private Config readConfig(Path configPath) throws IOException {
        Path configFilePath = configPath.resolve("config");
        File configFile = configFilePath.toFile();
        if (! configFile.exists()) {
            System.out.println("Unable to find ./ipfs/config file. Creating default config");
            String contents = defaultConfig();
            Files.write(configFilePath, contents.getBytes(), StandardOpenOption.CREATE);
            return new Config((HashMap) JSONParser.parse(contents));
        }
        String contents = Files.readString(configFilePath);
        return new Config((HashMap) JSONParser.parse(contents));
    }
    private String defaultConfig() {
        HostBuilder builder = new HostBuilder().generateIdentity();
        PrivKey privKey = builder.getPrivateKey();
        PeerId peerId = builder.getPeerId();

        List<String> swarmAddresses = List.of("/ip4/0.0.0.0/tcp/4001", "/ip6/::/tcp/4001");
        String apiAddress = "/ip4/127.0.0.1/tcp/5001";
        String gatewayAddress = "/ip4/127.0.0.1/tcp/8080";
        String proxyTargetAddress = "/ip4/127.0.0.1/tcp/8000";
        int bloomFilterSize = 0;

        Optional<String> allowTarget = Optional.empty(); // Optional.of("http://localhost:8000");
        List<String> bootstrapNodes = List.of(
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
                        "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                        "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
                        "/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ").stream()
                .collect(Collectors.toList());

        return buildConfig(privKey, peerId, bootstrapNodes, swarmAddresses, apiAddress, gatewayAddress, proxyTargetAddress,
                bloomFilterSize, allowTarget);
    }
    private String buildConfig(PrivKey privKey, PeerId peerId, List<String> bootstrapNodes, List<String> swarmAddresses, String apiAddress, String gatewayAddress,
                             String proxyTargetAddress, int bloomFilterSize, Optional<String> allowTarget) {
        Map configMap = new LinkedHashMap<>();

        Map addressesMap = new LinkedHashMap<>();
        addressesMap.put("API", apiAddress);
        addressesMap.put("Gateway", gatewayAddress);
        addressesMap.put("ProxyTarget", proxyTargetAddress);
        if (allowTarget.isPresent()) {
            addressesMap.put("AllowTarget", allowTarget.get());
        }
        addressesMap.put("Swarm", swarmAddresses);
        configMap.put("Addresses", addressesMap);

        configMap.put("Bootstrap", bootstrapNodes);

        Map datastoreMap = new LinkedHashMap<>();
        datastoreMap.put("BloomFilterSize", bloomFilterSize);

        Map blockMap = new LinkedHashMap<>();
        blockMap.put("mountpoint", "/blocks");
        blockMap.put("prefix", "flatfs.datastore");
        blockMap.put("type", "measure");
        Map blockChildMap = new LinkedHashMap<>();
        blockChildMap.put("path", "blocks");
        blockChildMap.put("shardFunc", "/repo/flatfs/shard/v1/next-to-last/2");
        blockChildMap.put("sync", "true");
        blockChildMap.put("type", "flatfs");
        blockMap.put("child", blockChildMap);

        Map dataMap = new LinkedHashMap<>();
        dataMap.put("mountpoint", "/");
        dataMap.put("prefix", "h2.datastore");
        dataMap.put("type", "measure");
        Map dataChildMap = new LinkedHashMap<>();
        dataChildMap.put("compression", "none");
        dataChildMap.put("path", "datastore");
        dataChildMap.put("type", "h2");
        dataMap.put("child", dataChildMap);

        List<Map> list = List.of(blockMap, dataMap);
        Map specMap = new LinkedHashMap<>();
        specMap.put("mounts", list);
        specMap.put("type", "mount");
        datastoreMap.put("Spec", specMap);
        configMap.put("Datastore", datastoreMap);

        Map experimentalMap = new LinkedHashMap<>();
        experimentalMap.put("Libp2pStreamMounting", true);
        experimentalMap.put("P2pHttpProxy", true);
        configMap.put("Experimental", experimentalMap);

        Map identityMap = new LinkedHashMap<>();
        identityMap.put("PeerID", peerId.toBase58());
        String base64PrivKeyStr = Base64.getEncoder().encodeToString(privKey.bytes());
        identityMap.put("PrivKey", base64PrivKeyStr);
        configMap.put("Identity", identityMap);

        Config config = new Config(configMap);
        return config.prettyPrint();
    }

    public static void main(String[] args) {
        new Server();
    }
}