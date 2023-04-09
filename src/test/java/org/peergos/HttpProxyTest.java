package org.peergos;

import com.sun.net.httpserver.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.netty.handler.codec.http.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

public class HttpProxyTest {

    @Test
    public void p2pProxyRequest() throws IOException {
        InetSocketAddress unusedProxyTarget = new InetSocketAddress("127.0.0.1", 7000);
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(unusedProxyTarget));
        Host node1 = builder1.build();
        InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", 8000);
        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(proxyTarget));
        Host node2 = builder2.build();
        node1.start().join();
        node2.start().join();

        // start local server with fixed HTTP response
        byte[] httpReply = "G'day from Java P2P HTTP proxy!".getBytes(StandardCharsets.UTF_8);
        HttpServer localhostServer = HttpServer.create(proxyTarget, 20);
        localhostServer.createContext("/", ex -> {
            ex.sendResponseHeaders(200, httpReply.length);
            ex.getResponseBody().write(httpReply);
            ex.getResponseBody().close();
            System.out.println("Target http server responded");
        });
        localhostServer.setExecutor(Executors.newSingleThreadExecutor());
        localhostServer.start();

        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            // send a p2p http request which should get proxied to the handler above by node2
            HttpProtocol.HttpController proxier = new HttpProtocol.Binding(unusedProxyTarget).dial(node1, address2)
                    .getController().join();
            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            for (int i=0; i < 10; i++) {
                long t1 = System.currentTimeMillis();
                FullHttpResponse resp = proxier.send(httpRequest.retain()).join();
                long t2 = System.currentTimeMillis();
                System.out.println("P2P HTTP request took " + (t2 - t1) + "ms");

                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                resp.content().readBytes(bout, resp.headers().getInt("content-length"));
                byte[] replyBody = bout.toByteArray();
                if (!Arrays.equals(replyBody, httpReply))
                    throw new IllegalStateException("Different http response!");
            }
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
