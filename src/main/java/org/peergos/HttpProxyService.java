package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.net.ProxyResponse;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.Logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class HttpProxyService {

    private static final Logger LOG = Logging.LOG();

    private final Host node;
    private final HttpProtocol.Binding nodeHttpBinding;
    public static final String API_URL = "/p2p/";

    public HttpProxyService(Host node, HttpProtocol.Binding nodeHttpBinding) {
        this.node = node;
        this.nodeHttpBinding = nodeHttpBinding;

    }
    public ProxyResponse proxyRequest(Multihash targetNodeId, String targetPath) throws IOException {
        return proxyRequest(targetNodeId, targetPath, Optional.empty());
    }

    public ProxyResponse proxyRequest(Multihash targetNodeId, String targetPath, Optional<byte[]> body) throws IOException {

        AddressBook addressBook = node.getAddressBook();
        Optional<Multiaddr> targetAddressesOpt = addressBook.get(PeerId.fromBase58(targetNodeId.toBase58())).join().stream().findFirst();
        if (targetAddressesOpt.isEmpty()) {
            LOG.info("Target not found in address book: " + targetNodeId);
            return new ProxyResponse(new byte[0], new HashMap<>(), 404);
        }
        HttpProtocol.HttpController proxier = nodeHttpBinding.dial(node, targetAddressesOpt.get())
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
