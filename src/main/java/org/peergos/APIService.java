package org.peergos;
import io.libp2p.core.Host;
import org.peergos.blockstore.Blockstore;
import com.sun.net.httpserver.*;
import org.peergos.net.APIHandler;
import org.peergos.util.Logging;
import org.peergos.util.Version;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class APIService {
	private static final Logger LOG = Logging.LOG();

    public static final Version CURRENT_VERSION = Version.parse("0.0.1");

    public APIService() {

    }

    public HttpServer initAndStart(InetSocketAddress local,
                                Host node,
                                Blockstore storage,
                                int connectionBacklog,
                                int handlerPoolSize) throws IOException {
        LOG.info("Starting RPC API server at: localhost:"+local.getPort());
        HttpServer localhostServer = HttpServer.create(local, connectionBacklog);
        localhostServer.createContext(Constants.API_URL, new APIHandler(storage, Constants.API_URL, node));
        localhostServer.setExecutor(Executors.newFixedThreadPool(handlerPoolSize));
        localhostServer.start();
        return localhostServer;
    }
}
