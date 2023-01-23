package org.peergos;

import com.sun.net.httpserver.*;
import io.ipfs.cid.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.netty.handler.codec.http.*;
import org.junit.*;
import org.peergos.bitswap.*;
import org.peergos.http.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;

public class HttpProxyTest {

    @Test
    public void getBlock() throws IOException {
        Bitswap bitswap1 = new Bitswap(new BitswapEngine(new RamBlockstore()));
        InetSocketAddress unusedProxyTarget = new InetSocketAddress("localhost", 7000);
        Host node1 = Server.buildHost(10000 + new Random().nextInt(50000), bitswap1, Optional.of(unusedProxyTarget));
        RamBlockstore blockstore2 = new RamBlockstore();
        InetSocketAddress proxyTarget = new InetSocketAddress("localhost", 8000);
        Host node2 = Server.buildHost(10000 + new Random().nextInt(50000), new Bitswap(new BitswapEngine(blockstore2)), Optional.of(proxyTarget));
        node1.start().join();
        node2.start().join();

        // start local server with fixed HTTP response
        byte[] httpReply = "G'day from Java P2P HTTP proxy!".getBytes(StandardCharsets.UTF_8);
        HttpServer localhostServer = HttpServer.create(proxyTarget, 20);
        localhostServer.createContext("/", ex -> {
            ex.sendResponseHeaders(200, httpReply.length);
            ex.getResponseBody().write(httpReply);
            ex.getResponseBody().close();
        });
        localhostServer.start();

        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            // send a p2p http request which should get proxied to the handler above by node2
            HttpProtocol.HttpController proxier = new HttpProtocol.Binding(unusedProxyTarget).dial(node1, address2)
                    .getController().join();
            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            FullHttpResponse resp = proxier.send(httpRequest).join();
            byte[] replyBody = resp.content().array();
            if (! Arrays.equals(replyBody, httpReply))
                throw new IllegalStateException("Different http response!");
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
