package org.peergos;

import com.sun.net.httpserver.*;
import io.ipfs.cid.Cid;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.netty.handler.codec.http.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

public class HttpProxyTest {

    @Test
    public void peerid() {
        String peerS = "z5AanNVJCxnFLuYe3Zz35BKv1noBKLToJexw29wQ8cXunqmoDi2sjRB";
        Cid decoded = Cid.decode(peerS);
        System.out.println(decoded);
    }

    @Test
    public void p2pProxyRequest() throws IOException {
        InetSocketAddress unusedProxyTarget = new InetSocketAddress("127.0.0.1", 7000);
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(unusedProxyTarget));
        Host node1 = builder1.build();
        InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", TestPorts.getPort());
        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(proxyTarget));
        Host node2 = builder2.build();
        node1.start().join();
        node2.start().join();

        // start local server with fixed HTTP response
        byte[] httpReply = new byte[1024*1024];
        new Random(42).nextBytes(httpReply);
        HttpServer localhostServer = HttpServer.create(proxyTarget, 20);
        localhostServer.createContext("/", ex -> {
            ex.sendResponseHeaders(200, httpReply.length);
            ex.getResponseBody().write(httpReply);
            ex.getResponseBody().close();
            //System.out.println("Target http server responded");
        });
        localhostServer.setExecutor(Executors.newSingleThreadExecutor());
        localhostServer.start();

        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            // send a p2p http request which should get proxied to the handler above by node2
            HttpProtocol.HttpController proxier = new HttpProtocol.Binding(unusedProxyTarget).dial(node1, address2)
                    .getController().join();
            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            long totalTime = 0;
            int count = 2000;
            for (int i = 0; i < count; i++) {
                long t1 = System.currentTimeMillis();
                FullHttpResponse resp = proxier.send(httpRequest.retain()).join();
                long t2 = System.currentTimeMillis();
                //System.out.println("P2P HTTP request took " + (t2 - t1) + "ms");
                totalTime += t2 - t1;

                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                resp.content().readBytes(bout, resp.headers().getInt("content-length"));
                resp.release();
                byte[] replyBody = bout.toByteArray();
                equal(replyBody, httpReply);
            }
            System.out.println("Average: " + totalTime / count);
        } finally {
            node1.stop();
            node2.stop();
        }
    }

    private static void equal(byte[] a, byte[] b) {
        if (a.length != b.length)
            throw new IllegalStateException("different lengths!");
        for (int i = 0; i < a.length; i++)
            if (a[i] != b[i]) {
                byte[] diff = Arrays.copyOfRange(a, i, i + 24);
                int j=0;
                for (;j < b.length-2;j++)
                    if (b[j] == diff[0] && b[j+1] == diff[1]&& b[j+2] == diff[2])
                        break;
                throw new IllegalStateException("bytes differ at " + i + " " + a[i] + " != " + b[i]);
            }
    }
}
