package org.peergos.client;

import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.peergos.util.JSONParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public final Block block = new Block();

    public final DHT dht = new DHT();

    public final Refs refs = new Refs();

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

    public Map id() throws IOException {
        return retrieveMap("id");
    }

    public class Refs {
        public List<Multihash> local() throws IOException {
            String jsonStream = new String(retrieve("refs/local"));
            return JSONParser.parseStream(jsonStream).stream()
                    .map(m -> (String) (((Map) m).get("Ref")))
                    .map(Cid::decode)
                    .collect(Collectors.toList());
        }
    }

    public class Block {
        public byte[] get(Multihash hash) throws IOException {
            return retrieve("block/get?stream-channels=true&arg=" + hash);
        }

        public byte[] rm(Multihash hash) throws IOException {
            return retrieve("block/rm?stream-channels=true&arg=" + hash);
        }

        public List<MerkleNode> put(List<byte[]> data) throws IOException {
            return put(data, Optional.empty());
        }

        public List<MerkleNode> put(List<byte[]> data, Optional<String> format) throws IOException {
            List<MerkleNode> res = new ArrayList<>();
            for (byte[] value : data) {
                res.add(put(value, format));
            }
            return res;
        }

        public MerkleNode put(byte[] data, Optional<String> format) throws IOException {
            String fmt = format.map(f -> "&format=" + f).orElse("");
            Multipart m = new Multipart(protocol +"://" + host + ":" + port + apiVersion+"block/put?stream-channels=true" + fmt, "UTF-8");
            try {
                m.addFilePart("file", Paths.get(""), new NamedStreamable.ByteArrayWrapper(data));
                String res = m.finish();
                return JSONParser.parseStream(res).stream().map(x -> MerkleNode.fromJSON((Map<String, Object>) x)).findFirst().get();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        public Map stat(Multihash hash) throws IOException {
            return retrieveMap("block/stat?stream-channels=true&arg=" + hash);
        }
    }


    public class DHT {
        @Deprecated
        public List<Map<String, Object>> findprovs(Multihash hash) throws IOException {
            return getAndParseStream("dht/findprovs?arg=" + hash).stream()
                    .map(x -> (Map<String, Object>) x)
                    .collect(Collectors.toList());
        }
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
