package org.peergos.net;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import org.peergos.blockstore.Blockstore;
import org.peergos.util.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;


public class APIHandler implements HttpHandler {
	private static final Logger LOG = Logging.LOG();

    private static final boolean LOGGING = true;

    private final Blockstore store;
    private final Host node;
    private final String apiPrefix;

    public static final String ID = "id";
    public static final String VERSION = "version";

    public APIHandler(Blockstore store,
                      String apiPrefix,
                      Host node) {
        this.store = store;
        this.apiPrefix = apiPrefix;
        this.node = node;
    }

    @Override
    public void handle(HttpExchange httpExchange) {
        try {
            if (!HttpUtil.allowedQuery(httpExchange)) {
                httpExchange.sendResponseHeaders(403, 0);
            } else {
                handleCallToAPI(httpExchange);
            }
        } catch (Exception e) {
            Throwable t = Exceptions.getRootCause(e);
            LOG.severe("Error handling " + httpExchange.getRequestURI());
            LOG.log(Level.WARNING, t.getMessage(), t);
            HttpUtil.replyError(httpExchange, t);
        }
    }
    public void handleCallToAPI(HttpExchange httpExchange) {

        long t1 = System.currentTimeMillis();
        String path = httpExchange.getRequestURI().getPath();
        try {
            if (! path.startsWith(apiPrefix))
                throw new IllegalStateException("Unsupported api version, required: " + apiPrefix);
            path = path.substring(apiPrefix.length());
            // N.B. URI.getQuery() decodes the query string
            Map<String, List<String>> params = HttpUtil.parseQuery(httpExchange.getRequestURI().getQuery());
            List<String> args = params.get("arg");
            Function<String, String> last = key -> params.get(key).get(params.get(key).size() - 1);

            switch (path) {
                case ID: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-id
                    PeerId peerId = node.getPeerId();
                    Map res = new HashMap<>();
                    res.put("ID",  peerId.toBase58());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case VERSION: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-version
                    Map res = new HashMap<>();
                    res.put("Version", "0.4.11");
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                default: {
                    httpExchange.sendResponseHeaders(404, 0);
                }
            }
        } catch (Exception e) {
            Throwable t = Exceptions.getRootCause(e);
            LOG.severe("Error handling " + httpExchange.getRequestURI());
            LOG.log(Level.WARNING, t.getMessage(), t);
            HttpUtil.replyError(httpExchange, t);
        } finally {
            httpExchange.close();
            long t2 = System.currentTimeMillis();
            if (LOGGING)
                LOG.info("DHT Handler handled " + path + " query in: " + (t2 - t1) + " mS");
        }
    }

    private static void replyJson(HttpExchange exchange, String json) {
        try {
            byte[] raw = json.getBytes();
            exchange.sendResponseHeaders(200, raw.length);
            DataOutputStream dout = new DataOutputStream(exchange.getResponseBody());
            dout.write(raw);
            dout.flush();
            dout.close();
        } catch (IOException e)
        {
            LOG.log(Level.WARNING, e.getMessage(), e);
        }
    }

    private static void replyBytes(HttpExchange exchange, byte[] body) {
        try {
            exchange.sendResponseHeaders(200, body.length);
            DataOutputStream dout = new DataOutputStream(exchange.getResponseBody());
            dout.write(body);
            dout.flush();
            dout.close();
        } catch (IOException e)
        {
            LOG.log(Level.WARNING, e.getMessage(), e);
        }
    }
}
