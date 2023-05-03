package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.net.ProxyRequest;
import org.peergos.net.ProxyResponse;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.Logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

public class HttpProxyService {

    private static final Logger LOG = Logging.LOG();
    private final Host node;
    private final HttpProtocol.Binding p2pHttpBinding;
    public static final String API_URL = "/p2p/";

    public HttpProxyService(Host node, HttpProtocol.Binding p2pHttpBinding) {
        this.node = node;
        this.p2pHttpBinding = p2pHttpBinding;
    }
    public ProxyResponse proxyRequest(Multihash targetNodeId, ProxyRequest request) throws IOException {

        AddressBook addressBook = node.getAddressBook();
        Optional<Multiaddr> targetAddressesOpt = addressBook.get(PeerId.fromBase58(targetNodeId.toBase58())).join().stream().findFirst();
        if (targetAddressesOpt.isEmpty()) {
            LOG.info("Target not found in address book: " + targetNodeId);
            return new ProxyResponse(new byte[0], new HashMap<>(), 404);
        }
        HttpProtocol.HttpController proxier = p2pHttpBinding.dial(node, targetAddressesOpt.get())
                .getController().join();
        String urlParams = constructQueryParamString(request.queryParams);
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.valueOf(request.method.name()),
                request.path + urlParams, request.body != null ?
                Unpooled.wrappedBuffer(request.body) : Unpooled.buffer(0));

        HttpHeaders reqHeaders = httpRequest.headers();
        for(Map.Entry<String, List<String>> entry : request.headers.entrySet()) {
            reqHeaders.set(entry.getKey(), entry.getValue());
        }
        reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, request.body != null ? request.body.length : 0);
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
    private String constructQueryParamString(Map<String, List<String>> queryParams) {
        StringBuilder sb = new StringBuilder();
        if (!queryParams.isEmpty()) {
            sb.append("?");
            for (Map.Entry<String, List<String>> entry: queryParams.entrySet()) {
                for(String value : entry.getValue()) {
                    sb.append(entry.getKey() + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8) + "&");
                }
            }
        }
        return sb.toString();
    }
}
