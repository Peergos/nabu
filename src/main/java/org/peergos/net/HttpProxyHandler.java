package org.peergos.net;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.PeerId;
import org.peergos.*;
import org.peergos.util.HttpUtil;
import org.peergos.util.Logging;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

public class HttpProxyHandler extends Handler {
	private static final Logger LOG = Logging.LOG();

    private static final boolean LOGGING = true;

    private final HttpProxyService service;

    private static final String HTTP_REQUEST = "/http/";

    public HttpProxyHandler(HttpProxyService service) {
        this.service = service;
    }


    public void handleCallToAPI(HttpExchange httpExchange) {
        long t1 = System.currentTimeMillis();
        String path = httpExchange.getRequestURI().getPath();
        try {
            if (path.startsWith(HttpProxyService.API_URL)) {
                // /p2p/$target_node_id/http/$target_path
                path = path.substring(HttpProxyService.API_URL.length());
                int streamPathIndex = path.indexOf('/');
                if (streamPathIndex == -1) {
                    throw new IllegalStateException("Expecting p2p request to include path in url");
                }
                String peerId = path.substring(0, streamPathIndex);
                Multihash targetNodeId = Multihash.deserialize(PeerId.fromBase58(peerId).getBytes());
                String targetPath = path.substring(streamPathIndex);
                if (!targetPath.startsWith(HTTP_REQUEST)) {
                    throw new IllegalStateException("Expecting path to be a http request");
                }
                targetPath = targetPath.substring(HTTP_REQUEST.length());
                byte[] body = read(httpExchange.getRequestBody());
                ProxyResponse response = service.proxyRequest(targetNodeId, targetPath, Optional.of(body));
                Headers headers = httpExchange.getResponseHeaders();
                for (Map.Entry<String, String> entry: response.headers.entrySet()) {
                    headers.replace(entry.getKey(), List.of(entry.getValue()));
                }
                httpExchange.sendResponseHeaders(response.statusCode, response.body.length);
                httpExchange.getResponseBody().write(response.body);
            } else {
                throw new IllegalStateException("Unsupported request");
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

    protected static byte[] read(InputStream in) throws IOException {
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
             OutputStream gout = new DataOutputStream(bout)) {
            byte[] tmp = new byte[4096];
            int r;
            while ((r = in.read(tmp)) >= 0)
                gout.write(tmp, 0, r);
            in.close();
            return bout.toByteArray();
        }
    }
}
