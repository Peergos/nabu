package org.peergos.net;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.*;
import io.libp2p.core.PeerId;
import org.peergos.*;
import org.peergos.blockstore.auth.Bat;
import org.peergos.blockstore.auth.BatId;
import org.peergos.cbor.*;
import org.peergos.protocol.ipns.*;
import org.peergos.util.*;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.*;

public class PeerAPIHandler extends Handler {
    public static final String API_URL = "/api/v0/";
    public static final Version CURRENT_VERSION = Version.parse("0.9.0");
    private static final Logger LOG = Logging.LOG();

    private static final boolean LOGGING = true;

    public static final String ID = "id";
    public static final String VERSION = "version";

    public static final String FIND_PROVS = "dht/findprovs";
    public static final String IPNS_GET = "ipns/get";

    private final EmbeddedPeer ipfs;

    public PeerAPIHandler(EmbeddedPeer ipfs) {
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
                case IPNS_GET: {
                    AggregatedMetrics.API_IPNS_GET.inc();
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"signer\" is required");
                    }
                    Multihash signer = Multihash.fromBase58(args.get(0));
                    List<IpnsRecord> records = ipfs.resolveRecords(signer, 1)
                            .stream()
                            .sorted()
                            .collect(Collectors.toList());
                    if (records.isEmpty())
                        throw new IllegalStateException("Couldn't resolve " + signer);
                    IpnsRecord latest = records.get(records.size() - 1);
                    IpnsMapping mapping = new IpnsMapping(signer, latest);
                    Map<String,  Object> res = new HashMap<>();
                    res.put("sig", ArrayOps.bytesToHex(mapping.getSignature()));
                    res.put("data", ArrayOps.bytesToHex(mapping.getData()));
                    String json = JSONParser.toString(res);
                    replyJson(httpExchange, json);
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
