package org.peergos.net;

import io.ipfs.cid.Cid;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import org.peergos.APIService;
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

    private final APIService service;
    private final Host node;

    public static final String ID = "id";
    public static final String VERSION = "version";
    public static final String GET = "/block/get";
    public static final String PUT = "/block/put";
    public static final String RM = "/block/rm";
    public static final String STAT = "/block/stat";
    public static final String REFS_LOCAL = "refs/local";
    public static final String BLOOM_ADD = "/bloom/add";
    public static final String HAS = "has"; // todo

    public APIHandler(APIService service, Host node) {
        this.service = service;
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
            if (! path.startsWith(APIService.API_URL))
                throw new IllegalStateException("Unsupported api version, required: " + APIService.API_URL);
            path = path.substring(APIService.API_URL.length());
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
                    res.put("Version", APIService.CURRENT_VERSION);
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case GET: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-get
                    if (args.size() != 1) {
                        throw new APIException("Missing arg parameter"); //todo confirm message
                    }
                    List<String> authz = params.get("auth");
                    Optional<String> authOpt = authz.size() == 1 ? Optional.of(authz.get(0)) : Optional.empty();
                    service.getBlock(Cid.decode(args.get(0)), authOpt).thenApply(blockOpt -> {
                        if (blockOpt.isPresent()) {
                            replyBytes(httpExchange, blockOpt.get());
                        } else {
                            try {
                                httpExchange.sendResponseHeaders(400, 0);
                            } catch (IOException ioe) {
                                Throwable t = Exceptions.getRootCause(ioe);
                                LOG.severe("Error handling " + httpExchange.getRequestURI());
                                LOG.log(Level.WARNING, t.getMessage(), t);
                                HttpUtil.replyError(httpExchange, t);
                            }
                        }
                        return null;
                    });
                    break;
                }
                case PUT: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-put
                    List<String> format = params.get("format");
                    Optional<String> formatOpt = format.size() == 1 ? Optional.of(format.get(0)) : Optional.empty();
                    if (formatOpt.isEmpty()) {
                        throw new APIException(""); //todo need to discuss
                    }
                    String boundary = httpExchange.getRequestHeaders().get("Content-Type")
                            .stream()
                            .filter(s -> s.contains("boundary="))
                            .map(s -> s.substring(s.indexOf("=") + 1))
                            .findAny()
                            .get();
                    List<byte[]> data = MultipartReceiver.extractFiles(httpExchange.getRequestBody(), boundary);
                    if (data.size() != 1) {
                        throw new APIException(""); //todo confirm message
                    }
                    byte[] block = data.get(0);
                    if (block.length >  1024 * 1024 * 2) { //todo what should the limit be?
                        throw new APIException(""); //todo confirm message
                    }
                    service.putBlock(block, formatOpt.get()).thenApply(cidOpt -> {
                        if (cidOpt.isPresent()) {
                            Map res = new HashMap<>();
                            res.put("Hash", cidOpt.get());
                            replyJson(httpExchange, JSONParser.toString(res));
                        } else {
                            try {
                                httpExchange.sendResponseHeaders(400, 0);
                            } catch (IOException ioe) {
                                Throwable t = Exceptions.getRootCause(ioe);
                                LOG.severe("Error handling " + httpExchange.getRequestURI());
                                LOG.log(Level.WARNING, t.getMessage(), t);
                                HttpUtil.replyError(httpExchange, t);
                            }
                        }
                        return null;
                    });
                    break;
                }
                case RM: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-rm
                    if (args.size() != 1) {
                        throw new APIException("Missing arg parameter"); //todo confirm message
                    }
                    Cid cid = Cid.decode(args.get(0));
                    service.rmBlock(cid).thenApply(deleted -> {
                        if (deleted) {
                            Map res = new HashMap<>();
                            res.put("Error", "");
                            res.put("Hash", cid);
                            replyJson(httpExchange, JSONParser.toString(res));
                        } else {
                            try {
                                httpExchange.sendResponseHeaders(400, 0);
                            } catch (IOException ioe) {
                                Throwable t = Exceptions.getRootCause(ioe);
                                LOG.severe("Error handling " + httpExchange.getRequestURI());
                                LOG.log(Level.WARNING, t.getMessage(), t);
                                HttpUtil.replyError(httpExchange, t);
                            }
                        }
                        return null;
                    });
                    break;
                }
                case STAT: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-stat
                    if (args.size() != 1) {
                        throw new APIException("Missing arg parameter"); //todo confirm message
                    }
                    List<String> authz = params.get("auth");
                    Optional<String> authOpt = authz.size() == 1 ? Optional.of(authz.get(0)) : Optional.empty();
                    service.getBlock(Cid.decode(args.get(0)), authOpt).thenApply(blockOpt -> {
                        if (blockOpt.isPresent()) {
                            Map res = new HashMap<>();
                            res.put("Size", blockOpt.get().length);
                            replyJson(httpExchange, JSONParser.toString(res));
                        } else {
                            try {
                                httpExchange.sendResponseHeaders(400, 0);
                            } catch (IOException ioe) {
                                Throwable t = Exceptions.getRootCause(ioe);
                                LOG.severe("Error handling " + httpExchange.getRequestURI());
                                LOG.log(Level.WARNING, t.getMessage(), t);
                                HttpUtil.replyError(httpExchange, t);
                            }
                        }
                        return null;
                    });
                    break;
                }
                case REFS_LOCAL: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-refs-local
                    service.getRefs().thenApply(blockOpt -> {
                        // todo
                        return null;
                    });
                    break;
                }
                case HAS: {
                    if (args.size() != 1) {
                        throw new APIException("Missing arg parameter"); //todo confirm message
                    }
                    service.hasBlock(Cid.decode(args.get(0))).thenApply(has -> {
                        replyBytes(httpExchange, has ? "true".getBytes() : "false".getBytes());
                        return null;
                    });
                    break;
                }
                case BLOOM_ADD: {
                    if (args.size() != 1) {
                        throw new APIException("Missing arg parameter"); //todo confirm message
                    }
                    service.bloomAdd(Cid.decode(args.get(0))).thenApply(done -> {
                        replyBytes(httpExchange, "".getBytes());
                        return null;
                    });
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
