package org.peergos.client;

import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.PeerId;
import org.peergos.PeerAddresses;
import org.peergos.util.JSONParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class NabuClient {

    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;
    private static final int DEFAULT_READ_TIMEOUT_MILLIS = 60_000;

    public final String host;
    public final int port;
    public final String protocol;
    private final String apiVersion;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;

    public NabuClient(String host, int port) {
        this(host, port, "/api/v0/", false);
    }

    public NabuClient(String multiaddr) {
        this(new MultiAddress(multiaddr));
    }

    public NabuClient(MultiAddress addr) {
        this(addr.getHost(), addr.getPort(), "/api/v0/", detectSSL(addr));
    }

    public NabuClient(String host, int port, String version, boolean ssl) {
        this(host, port, version, DEFAULT_CONNECT_TIMEOUT_MILLIS, DEFAULT_READ_TIMEOUT_MILLIS, ssl);
    }

    public NabuClient(String host, int port, String version, int connectTimeoutMillis, int readTimeoutMillis, boolean ssl) {
        if (connectTimeoutMillis < 0) throw new IllegalArgumentException("connect timeout must be zero or positive");
        if (readTimeoutMillis < 0) throw new IllegalArgumentException("read timeout must be zero or positive");
        this.host = host;
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;

        if (ssl) {
            this.protocol = "https";
        } else {
            this.protocol = "http";
        }

        this.apiVersion = version;
    }

    public NabuClient timeout(int timeout) {
        return new NabuClient(host, port, apiVersion, timeout, timeout, protocol.equals("https"));
    }

    public String version() throws IOException {
        Map m = (Map)retrieveAndParse("version");
        return (String)m.get("Version");
    }

    public PeerId id() throws IOException {
        Map<String, String> res = retrieveMap("id");
        return PeerId.fromBase58(res.get("ID"));
    }

    public List<Cid> listBlockstore() throws IOException {
        String jsonStream = new String(retrieve("refs/local"));
        return JSONParser.parseStream(jsonStream).stream()
                .map(m -> (String) (((Map) m).get("Ref")))
                .map(Cid::decode)
                .collect(Collectors.toList());
    }

    public boolean hasBlock(Multihash hash, Optional<String> auth) throws IOException {
        String authArg = auth.isPresent() ? "&auth=" + auth.get() : "";
        return "true".equals(new String(retrieve("block/has?arg=" + hash + authArg)));
    }

    public boolean bloomAdd(Multihash hash) throws IOException {
        return "true".equals(new String(retrieve("bloom/add?arg=" + hash)));
    }

    public byte[] getBlock(Multihash hash, Optional<String> auth) throws IOException {
        String authArg = auth.isPresent() ? "&auth=" + auth.get() : "";
        return retrieve("block/get?arg=" + hash + authArg);
    }

    public void removeBlock(Multihash hash) throws IOException {
        retrieve("block/rm?arg=" + hash);
    }

    public List<Cid> putBlocks(List<byte[]> data) throws IOException {
        return putBlocks(data, Optional.empty());
    }

    public List<Cid> putBlocks(List<byte[]> data, Optional<String> format) throws IOException {
        List<Cid> res = new ArrayList<>();
        for (byte[] value : data) {
            res.add(putBlock(value, format));
        }
        return res;
    }

    public Cid putBlock(byte[] data, Optional<String> format) throws IOException {
        String fmt = format.map(f -> "&format=" + f).orElse("");
        Multipart m = new Multipart(protocol +"://" + host + ":" + port + apiVersion+"block/put?stream-channels=true" + fmt, "UTF-8");
        try {
            m.addFilePart("file", Paths.get(""), new NamedStreamable.ByteArrayWrapper(data));
            String res = m.finish();
            LinkedHashMap<String, String> obj = (LinkedHashMap<String, String>)JSONParser.parse(res);
            return Cid.decode(obj.get("Hash"));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public int stat(Multihash hash) throws IOException {
        Map<String, Integer> res = retrieveMap("block/stat?arg=" + hash);
        return res.get("Size");
    }

    public List<PeerAddresses> findProviders(Multihash hash) throws IOException {
        List<Map<String, Object>> results = getAndParseStream("dht/findprovs?arg=" + hash).stream()
                .map(x -> (Map<String, Object>) x)
                .collect(Collectors.toList());
        List<PeerAddresses> providers = new ArrayList<>();
        for (Map<String, Object> entry : results) {
            Map<String, Object> responses = (Map<String, Object>)entry.get("Responses");
            Multihash peerId = Multihash.fromBase58((String) responses.get("ID"));
            ArrayList<String> addrs = (ArrayList<String>)responses.get("Addrs");
            List<MultiAddress> peerAddresses = addrs.stream().map(a -> new MultiAddress(a)).collect(Collectors.toList());
            providers.add(new PeerAddresses(peerId, peerAddresses));
        }
        return providers;
    }

    private Map retrieveMap(String path) throws IOException {
        return (Map)retrieveAndParse(path);
    }

    private Object retrieveAndParse(String path) throws IOException {
        byte[] res = retrieve(path);
        return JSONParser.parse(new String(res));
    }

    private byte[] retrieve(String path) throws IOException {
        URL target = new URL(protocol, host, port, apiVersion + path);
        return get(target, connectTimeoutMillis, readTimeoutMillis);
    }

    private static byte[] get(URL target, int connectTimeoutMillis, int readTimeoutMillis) throws IOException {
        HttpURLConnection conn = configureConnection(target, "POST", connectTimeoutMillis, readTimeoutMillis);
        conn.setDoOutput(true);
        /* See IPFS commit for why this is a POST and not a GET https://github.com/ipfs/go-ipfs/pull/7097
           This commit upgrades go-ipfs-cmds and configures the commands HTTP API Handler
           to only allow POST/OPTIONS, disallowing GET and others in the handling of
           command requests in the IPFS HTTP API (where before every type of request
           method was handled, with GET/POST/PUT/PATCH being equivalent).

           The Read-Only commands that the HTTP API attaches to the gateway endpoint will
           additional handled GET as they did before (but stop handling PUT,DELETEs).

           By limiting the request types we address the possibility that a website
           accessed by a browser abuses the IPFS API by issuing GET requests to it which
           have no Origin or Referrer set, and are thus bypass CORS and CSRF protections.

           This is a breaking change for clients that relay on GET requests against the
           HTTP endpoint (usually :5001). Applications integrating on top of the
           gateway-read-only API should still work (including cross-domain access).
        */
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");

        try {
            OutputStream out = conn.getOutputStream();
            out.write(new byte[0]);
            out.flush();
            out.close();
            InputStream in = conn.getInputStream();
            ByteArrayOutputStream resp = new ByteArrayOutputStream();

            byte[] buf = new byte[4096];
            int r;
            while ((r = in.read(buf)) >= 0)
                resp.write(buf, 0, r);
            return resp.toByteArray();
        } catch (ConnectException e) {
            throw new RuntimeException("Couldn't connect to IPFS daemon at "+target+"\n Is IPFS running?");
        } catch (IOException e) {
            throw extractError(e, conn);
        }
    }


    private List<Object> getAndParseStream(String path) throws IOException {
        InputStream in = retrieveStream(path);
        byte LINE_FEED = (byte)10;

        ByteArrayOutputStream resp = new ByteArrayOutputStream();

        byte[] buf = new byte[4096];
        int r;
        List<Object> res = new ArrayList<>();
        while ((r = in.read(buf)) >= 0) {
            resp.write(buf, 0, r);
            if (buf[r - 1] == LINE_FEED) {
                res.add(JSONParser.parse(new String(resp.toByteArray())));
                resp.reset();
            }
        }
        return res;
    }

    private InputStream retrieveStream(String path) throws IOException {
        URL target = new URL(protocol, host, port, apiVersion + path);
        return NabuClient.getStream(target, connectTimeoutMillis, readTimeoutMillis);
    }

    private static InputStream getStream(URL target, int connectTimeoutMillis, int readTimeoutMillis) throws IOException {
        HttpURLConnection conn = configureConnection(target, "POST", connectTimeoutMillis, readTimeoutMillis);
        try {
            return conn.getInputStream();
        } catch (IOException e) {
            throw extractError(e, conn);
        }
    }

    public static RuntimeException extractError(IOException e, HttpURLConnection conn) {
        InputStream errorStream = conn.getErrorStream();
        String err = errorStream == null ? e.getMessage() : new String(readFully(errorStream));
        return new RuntimeException("IOException contacting IPFS daemon.\n"+err+"\nTrailer: " + conn.getHeaderFields().get("Trailer"), e);
    }

    private static final byte[] readFully(InputStream in) {
        try {
            ByteArrayOutputStream resp = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int r;
            while ((r=in.read(buf)) >= 0)
                resp.write(buf, 0, r);
            return resp.toByteArray();

        } catch(IOException ex) {
            throw new RuntimeException("Error reading InputStrean", ex);
        }
    }

    private static boolean detectSSL(MultiAddress multiaddress) {
        return multiaddress.toString().contains("/https");
    }

    private static HttpURLConnection configureConnection(URL target, String method, int connectTimeoutMillis, int readTimeoutMillis) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) target.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setConnectTimeout(connectTimeoutMillis);
        conn.setReadTimeout(readTimeoutMillis);
        return conn;
    }
}
