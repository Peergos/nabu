package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.net.ProxyException;
import org.peergos.net.ProxyRequest;
import org.peergos.net.ProxyResponse;
import org.peergos.protocol.dht.Kademlia;
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
    private final Kademlia dht;
    public static final String API_URL = "/p2p/";

    public HttpProxyService(Host node, HttpProtocol.Binding p2pHttpBinding, Kademlia dht) {
        this.node = node;
        this.p2pHttpBinding = p2pHttpBinding;
        this.dht = dht;
    }
    public ProxyResponse proxyRequest(Multihash targetNodeId, ProxyRequest request) throws IOException, ProxyException {

        AddressBook addressBook = node.getAddressBook();
        PeerId peerId = PeerId.fromBase58(targetNodeId.bareMultihash().toBase58());
        Optional<Multiaddr> targetAddressesOpt = addressBook.get(peerId).join().stream().findFirst();
        Multiaddr[] allAddresses = null;
        if (targetAddressesOpt.isEmpty()) {
            List<PeerAddresses> closestPeers = dht.findClosestPeers(targetNodeId, 20, node);
            Optional<PeerAddresses> matching = closestPeers.stream().filter(p -> p.peerId.equals(targetNodeId)).findFirst();
            if (matching.isEmpty()) {
                throw new ProxyException("Target not found: " + targetNodeId);
            }
            PeerAddresses peer = matching.get();
            allAddresses = peer.getPublicAddresses().stream().map(a -> Multiaddr.fromString(a.toString())).toArray(Multiaddr[]::new);
        }
        Multiaddr[] addressesToDial = targetAddressesOpt.isPresent() ?
                Arrays.asList(targetAddressesOpt.get()).toArray(Multiaddr[]::new)
                : allAddresses;
        HttpProtocol.HttpController proxier = p2pHttpBinding.dial(node, peerId, addressesToDial).getController().join();
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
            String key = entry.getKey();
            if (key != null) {
                headers.put(key, entry.getValue());
            }
        }
        int code = resp.status().code();
        resp.release();
        return new ProxyResponse(bout.toByteArray(), headers, code);
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
