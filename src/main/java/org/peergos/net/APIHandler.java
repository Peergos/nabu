package org.peergos.net;

import io.ipfs.cid.Cid;
import io.libp2p.core.PeerId;
import org.peergos.*;
import org.peergos.util.*;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.*;

public class APIHandler extends Handler {
    public static final String API_URL = "/api/v0/";
    public static final Version CURRENT_VERSION = Version.parse("0.0.1");
    private static final Logger LOG = Logging.LOG();

    private static final boolean LOGGING = true;

    public static final String ID = "id";
    public static final String VERSION = "version";
    public static final String GET = "block/get";
    public static final String PUT = "block/put";
    public static final String RM = "block/rm";
    public static final String STAT = "block/stat";
    public static final String REFS_LOCAL = "refs/local";
    public static final String BLOOM_ADD = "bloom/add";
    public static final String HAS = "block/has";

    public static final String FIND_PROVS = "dht/findprovs";

    private final EmbeddedIpfs ipfs;

    public APIHandler(EmbeddedIpfs ipfs) {
        this.ipfs = ipfs;
    }

    public void handleCallToAPI(HttpExchange httpExchange) {

        long t1 = System.currentTimeMillis();
        String path = httpExchange.getRequestURI().getPath();
        try {
            if (! path.startsWith(API_URL))
                throw new IllegalStateException("Unsupported api version, required: " + API_URL);
            path = path.substring(API_URL.length());
            // N.B. URI.getQuery() decodes the query string
            Map<String, List<String>> params = HttpUtil.parseQuery(httpExchange.getRequestURI().getQuery());
            List<String> args = params.get("arg");

            switch (path) {
                case ID: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-id
                    AggregatedMetrics.API_ID.inc();
                    PeerId peerId = ipfs.node.getPeerId();
                    Map res = new HashMap<>();
                    res.put("ID",  peerId.toBase58());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case VERSION: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-version
                    AggregatedMetrics.API_VERSION.inc();
                    Map res = new HashMap<>();
                    res.put("Version", CURRENT_VERSION.toString());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case GET: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-get
                    AggregatedMetrics.API_BLOCK_GET.inc();
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"ipfs-path\" is required");
                    }
                    Optional<String> auth = Optional.ofNullable(params.get("auth"))
                            .map(a -> a.get(0))
                            .flatMap(a -> a.isEmpty() ? Optional.empty() : Optional.of(a));
                    Set<PeerId> peers = Optional.ofNullable(params.get("peers"))
                            .map(p -> p.stream().map(PeerId::fromBase58).collect(Collectors.toSet()))
                            .orElse(Collections.emptySet());
                    boolean addToBlockstore = Optional.ofNullable(params.get("persist"))
                            .map(a -> Boolean.parseBoolean(a.get(0)))
                            .orElse(true);
                    List<HashedBlock> block = ipfs.getBlocks(List.of(new Want(Cid.decode(args.get(0)), auth)), peers, addToBlockstore);
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
                    AggregatedMetrics.API_BLOCK_PUT.inc();
                    List<String> format = params.get("format");
                    Optional<String> formatOpt = format !=null && format.size() == 1 ? Optional.of(format.get(0)) : Optional.empty();
                    if (formatOpt.isEmpty()) {
                        throw new APIException("argument \"format\" is required");
                    }
                    String reqFormat = formatOpt.get().toLowerCase();
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
                    Cid cid = ipfs.blockstore.put(block, Cid.Codec.lookupIPLDName(reqFormat)).join();
                    Map res = new HashMap<>();
                    res.put("Hash", cid.toString());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case RM: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-rm
                    AggregatedMetrics.API_BLOCK_RM.inc();
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Cid cid = Cid.decode(args.get(0));
                    boolean deleted = ipfs.blockstore.rm(cid).join();
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
                    AggregatedMetrics.API_BLOCK_STAT.inc();
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Optional<String> auth = Optional.ofNullable(params.get("auth")).map(a -> a.get(0));
                    List<HashedBlock> block = ipfs.getBlocks(List.of(new Want(Cid.decode(args.get(0)), auth)), Collections.emptySet(), false);
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
                    AggregatedMetrics.API_REFS_LOCAL.inc();
                    List<Cid> refs = ipfs.blockstore.refs().join();
                    StringBuilder sb = new StringBuilder();
                    for (Cid cid : refs) {
                        Map<String, String> entry = new HashMap<>();
                        entry.put("Ref", cid.toString());
                        entry.put("Err", "");
                        sb.append(JSONParser.toString(entry));
                    }
                    replyBytes(httpExchange, sb.toString().getBytes());
                    break;
                }
                case HAS: {
                    AggregatedMetrics.API_BLOCK_HAS.inc();
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"ipfs-path\" is required");
                    }
                    boolean has = ipfs.blockstore.has(Cid.decode(args.get(0))).join();
                    replyBytes(httpExchange, has ? "true".getBytes() : "false".getBytes());
                    break;
                }
                case BLOOM_ADD: {
                    AggregatedMetrics.API_BLOOM_ADD.inc();
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Boolean added = ipfs.blockstore.bloomAdd(Cid.decode(args.get(0))).join();
                    replyBytes(httpExchange, added.toString().getBytes());
                    break;
                }
                case FIND_PROVS: {
                    AggregatedMetrics.API_FIND_PROVS.inc();
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Optional<Integer> providersParam = Optional.ofNullable(params.get("num-providers")).map(a -> Integer.parseInt(a.get(0)));
                    int numProviders = providersParam.isPresent() && providersParam.get() > 0 ? providersParam.get() : 20;
                    List<PeerAddresses> providers = ipfs.dht.findProviders(Cid.decode(args.get(0)), ipfs.node, numProviders).join();
                    StringBuilder sb = new StringBuilder();
                    Map<String, Object> entry = new HashMap<>();
                    Map<String, Object> responses = new HashMap<>();
                    for (PeerAddresses provider : providers) {
                        List<String> addresses = provider.addresses.stream().map(a -> a.toString()).collect(Collectors.toList());
                        responses.put("Addrs", addresses);
                        responses.put("ID", provider.peerId.toBase58());
                    }
                    entry.put("Responses", responses);
                    sb.append(JSONParser.toString(entry) + "\n");
                    replyBytes(httpExchange, sb.toString().getBytes());
                    break;
                }
                default: {
                    httpExchange.sendResponseHeaders(404, 0);
                    break;
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
}
