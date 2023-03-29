package org.peergos.net;

import io.ipfs.cid.Cid;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import org.peergos.*;
import org.peergos.util.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.*;


public class APIHandler implements HttpHandler {
	private static final Logger LOG = Logging.LOG();

    private static final boolean LOGGING = true;

    private final APIService service;
    private final Host node;

    public static final String ID = "id";
    public static final String VERSION = "version";
    public static final String GET = "block/get";
    public static final String PUT = "block/put";
    public static final String RM = "block/rm";
    public static final String STAT = "block/stat";
    public static final String REFS_LOCAL = "refs/local";
    public static final String BLOOM_ADD = "bloom/add";
    public static final String HAS = "block/has";

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
                    res.put("Version", APIService.CURRENT_VERSION.toString());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case GET: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-get
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"ipfs-path\" is required");
                    }
                    Optional<String> auth = Optional.ofNullable(params.get("auth")).map(a -> a.get(0));
                    Set<PeerId> peers = Optional.ofNullable(params.get("peers"))
                            .map(p -> p.stream().map(PeerId::fromBase58).collect(Collectors.toSet()))
                            .orElse(Collections.emptySet());
                    boolean addToBlockstore = Optional.ofNullable(params.get("persist"))
                            .map(a -> Boolean.parseBoolean(a.get(0)))
                            .orElse(true);
                    List<HashedBlock> block = service.getBlocks(List.of(new Want(Cid.decode(args.get(0)), auth)), peers, addToBlockstore);
                    if (! block.isEmpty()) {
                        replyBytes(httpExchange, block.get(0).block);
                    } else {
                        try {
                            httpExchange.sendResponseHeaders(400, 0);
                        } catch (IOException ioe) {
                            HttpUtil.replyError(httpExchange, ioe);
                        }
                    }
                    break;
                }
                case PUT: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-put
                    List<String> format = params.get("format");
                    Optional<String> formatOpt = format !=null && format.size() == 1 ? Optional.of(format.get(0)) : Optional.empty();
                    if (formatOpt.isEmpty()) {
                        throw new APIException("argument \"format\" is required");
                    } else {
                        String reqFormat = formatOpt.get();
                        if (!(reqFormat.equals("raw") || reqFormat.equals("cbor"))) {
                            throw new APIException("only raw and cbor \"format\" supported");
                        }
                    }
                    String boundary = httpExchange.getRequestHeaders().get("Content-Type")
                            .stream()
                            .filter(s -> s.contains("boundary="))
                            .map(s -> s.substring(s.indexOf("=") + 1))
                            .findAny()
                            .get();
                    List<byte[]> data = MultipartReceiver.extractFiles(httpExchange.getRequestBody(), boundary);
                    if (data.size() != 1) {
                        throw new APIException("Multiple input not supported");
                    }
                    byte[] block = data.get(0);
                    if (block.length >  1024 * 1024 * 2) { //todo what should the limit be?
                        throw new APIException("Block too large");
                    }
                    Cid cid = service.putBlock(block, formatOpt.get());
                    Map res = new HashMap<>();
                    res.put("Hash", cid.toString());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case RM: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-rm
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Cid cid = Cid.decode(args.get(0));
                    boolean deleted = service.rmBlock(cid);
                    if (deleted) {
                        Map res = new HashMap<>();
                        res.put("Error", "");
                        res.put("Hash", cid.toString());
                        replyJson(httpExchange, JSONParser.toString(res));
                    } else {
                        try {
                            httpExchange.sendResponseHeaders(400, 0);
                        } catch (IOException ioe) {
                            HttpUtil.replyError(httpExchange, ioe);
                        }
                    }
                    break;
                }
                case STAT: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-stat
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Optional<String> auth = Optional.ofNullable(params.get("auth")).map(a -> a.get(0));
                    List<HashedBlock> block = service.getBlocks(List.of(new Want(Cid.decode(args.get(0)), auth)), Collections.emptySet(), false);
                    if (! block.isEmpty()) {
                        Map res = new HashMap<>();
                        res.put("Size", block.get(0).block.length);
                        replyJson(httpExchange, JSONParser.toString(res));
                    } else {
                        try {
                            httpExchange.sendResponseHeaders(400, 0);
                        } catch (IOException ioe) {
                            HttpUtil.replyError(httpExchange, ioe);
                        }
                    }
                    break;
                }
                case REFS_LOCAL: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-refs-local
                    List<Cid> refs = service.getRefs();
                    StringBuilder sb = new StringBuilder();
                    for (Cid cid : refs) {
                        Map entry = new HashMap<>();
                        entry.put("Ref", cid.toString());
                        entry.put("Err", "");
                        sb.append(JSONParser.toString(entry));
                    }
                    replyBytes(httpExchange, sb.toString().getBytes());
                    break;
                }
                case HAS: {
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"ipfs-path\" is required");
                    }
                    boolean has = service.hasBlock(Cid.decode(args.get(0)));
                    replyBytes(httpExchange, has ? "true".getBytes() : "false".getBytes());
                    break;
                }
                case BLOOM_ADD: {
                    if (args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    boolean done = service.bloomAdd(Cid.decode(args.get(0)));
                    replyBytes(httpExchange, "".getBytes());
                    break;
                }
                default: {
                    httpExchange.sendResponseHeaders(404, 0);
                }
            }
        } catch (Exception e) {
            HttpUtil.replyError(httpExchange, e);
        } finally {
            httpExchange.close();
            long t2 = System.currentTimeMillis();
            if (LOGGING)
                LOG.info("API Handler handled " + path + " query in: " + (t2 - t1) + " mS");
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
