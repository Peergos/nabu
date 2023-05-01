package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.net.ProxyResponse;
import org.peergos.protocol.http.HttpProtocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HttpProxyService {

    private final Host node;
    public static final String API_URL = "/p2p/";

    public HttpProxyService(Host node) {
        this.node = node;
    }
    public ProxyResponse proxyRequest(Multihash targetNodeId, String targetPath) throws IOException {
        return proxyRequest(targetNodeId, targetPath, Optional.empty());
    }

    public ProxyResponse proxyRequest(Multihash targetNodeId, String targetPath, Optional<byte[]> body) throws IOException {

        Multiaddr address2 = null; //fixme
        InetSocketAddress unusedProxyTarget = new InetSocketAddress("127.0.0.1", 7000); //fixme
        HttpProtocol.HttpController proxier = new HttpProtocol.Binding(unusedProxyTarget).dial(node, address2)
                .getController().join();

        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                targetPath, body.isPresent() ?
                Unpooled.wrappedBuffer(body.get()) : Unpooled.buffer(0));
        httpRequest.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.isPresent() ? body.get().length : 0);
        FullHttpResponse resp = proxier.send(httpRequest.retain()).join();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        int contentLength = resp.headers().getInt("content-length");
        resp.content().readBytes(bout, contentLength);
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, String> entry: resp.headers().entries()) {
            headers.put(entry.getKey(), entry.getValue());
        }
        return new ProxyResponse(bout.toByteArray(), headers, resp.status().code());
    }

}
