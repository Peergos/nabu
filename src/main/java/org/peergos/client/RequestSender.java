package org.peergos.client;

import io.ipfs.multiaddr.MultiAddress;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestSender {

    public static FullHttpResponse proxy(MultiAddress proxyTargetAddress, FullHttpRequest request) throws IOException {
        HttpMethod method = request.method();
        String uri = request.uri();
        ByteBuf content = request.content();
        HttpHeaders headers = request.headers();
        Map<String, String> reqHeaders = new HashMap<>();
        for(Map.Entry<String, String> entry : headers.entries()) {
            reqHeaders.put(entry.getKey(), entry.getValue());
        }
        URL target = new URL("http", proxyTargetAddress.getHost(), proxyTargetAddress.getPort(), uri);
        Response reply = send(target, method.name(), content.array(), reqHeaders);

        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(reply.statusCode),
                reply.body.length > 0 ?
                        Unpooled.wrappedBuffer(reply.body) : Unpooled.buffer(0));
        for (Map.Entry<String, List<String>> entry : reply.responseHeaders.entrySet()) {
            String key = entry.getKey();
            if (key != null) {
                httpResponse.headers().set(key, entry.getValue());
            }
        }
        return httpResponse;
    }
    public static class Response {
        public final byte[] body;
        public final Map<String, List<String>> responseHeaders;
        public final int statusCode;

        public Response(byte[] body, Map<String, List<String>> responseHeaders, int statusCode) {
            this.body = body;
            this.responseHeaders = responseHeaders;
            this.statusCode = statusCode;
        }
    }
    public static Response send(URL target, String method, byte[] body, Map<String, String> headers) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) target.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod(method);
        for (Map.Entry<String, String> entry: headers.entrySet()) {
            conn.setRequestProperty(entry.getKey(), entry.getValue());
        }
        if (body.length > 0) {
            OutputStream out = conn.getOutputStream();
            out.write(body);
            out.flush();
            out.close();
        }
        InputStream in = conn.getInputStream();
        ByteArrayOutputStream resp = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int r;
        while ((r = in.read(buf)) >= 0)
            resp.write(buf, 0, r);
        Map<String, List<String>> map = Collections.unmodifiableMap(conn.getHeaderFields());
        return new Response(resp.toByteArray(), map, conn.getResponseCode());
    }
}
