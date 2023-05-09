package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.multiaddr.MultiAddress;
import org.peergos.config.*;
import org.peergos.net.APIHandler;
import org.peergos.net.HttpProxyHandler;
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

public class Server {

    private static final Logger LOG = Logger.getLogger(Server.class.getName());

    public Server(Args args) throws Exception {
        Path ipfsPath = getIPFSPath(args);
        Logging.init(ipfsPath, args.getBoolean("logToConsole", false));
        Config config = readConfig(ipfsPath);
        LOG.info("Starting Nabu version: " + APIService.CURRENT_VERSION);

        EmbeddedIpfs ipfs = EmbeddedIpfs.build(config, ipfsPath);

        String apiAddressArg = "Addresses.API";
        MultiAddress apiAddress = args.hasArg(apiAddressArg) ? new MultiAddress(args.getArg(apiAddressArg)) :  config.addresses.apiAddress;
        InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

        int maxConnectionQueue = 500;
        int handlerThreads = 50;
        LOG.info("Starting RPC API server at " + apiAddress.getHost() + ":" + localAPIAddress.getPort());
        HttpServer apiServer = HttpServer.create(localAPIAddress, maxConnectionQueue);

        APIService service = new APIService(ipfs.blockstore, new BitswapBlockService(ipfs.node, ipfs.bitswap), ipfs.dht);
        apiServer.createContext(APIService.API_URL, new APIHandler(service, ipfs.node));
        apiServer.createContext(HttpProxyService.API_URL, new HttpProxyHandler(new HttpProxyService(ipfs.node, ipfs.p2pHttp, ipfs.dht)));
        apiServer.setExecutor(Executors.newFixedThreadPool(handlerThreads));
        apiServer.start();

        Thread shutdownHook = new Thread(() -> {
            LOG.info("Stopping server...");
            try {
                ipfs.stop().join();
                apiServer.stop(3); //wait max 3 seconds
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