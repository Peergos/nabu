package org.peergos;

import com.sun.net.httpserver.*;
import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.netty.handler.codec.http.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;
import org.peergos.util.*;
import org.peergos.util.HttpUtil;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.*;

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
        String headerName = "Random-Header";
        String headerValue = "bananas";
        localhostServer.createContext("/", ex -> {
            ex.getResponseHeaders().set(headerName, headerValue);
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

            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            long totalTime = 0;
            int count = 2000;
            for (int i = 0; i < count; i++) {
                HttpProtocol.HttpController proxier = new HttpProtocol.Binding(unusedProxyTarget).dial(node1, address2)
                        .getController().join();
                long t1 = System.currentTimeMillis();
                FullHttpResponse resp = proxier.send(httpRequest.retain()).join();
                long t2 = System.currentTimeMillis();
                //System.out.println("P2P HTTP request took " + (t2 - t1) + "ms");
                totalTime += t2 - t1;

                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                resp.content().readBytes(bout, resp.headers().getInt("content-length"));
                Assert.assertTrue(resp.headers().get(headerName).equals(headerValue));
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

    @Test
    public void p2pProxyClientTest() throws Exception {
        InetSocketAddress unusedProxyTarget = new InetSocketAddress("127.0.0.1", 7000);
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(unusedProxyTarget));
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1);
        Kademlia dht = builder1.getWanDht().get();
        dht.bootstrapRoutingTable(node1, BootstrapTest.BOOTSTRAP_NODES, a -> true);
        System.out.println("Bootstrapping node...");
        long t0 = System.currentTimeMillis();
        dht.bootstrap(node1);
        System.out.println("Done in " + (System.currentTimeMillis() - t0) + "mS");

        try {
            String peerId = "QmUUv85Z8fq5VMBDVRZfSVVrNKss5J5M2j17mB3CWVxK78";
//            String peerId = "QmVdFZgHnEgcedCS2G2ZNiEN59LuVrnRm7z3yXtEBv2XiF";
            List<PeerAddresses> closestPeers = dht.findClosestPeers(Multihash.fromBase58(peerId), 1, node1);
//            Multiaddr address2 = new Multiaddr("/ip4/50.116.48.246/tcp/4001/p2p/QmUUv85Z8fq5VMBDVRZfSVVrNKss5J5M2j17mB3CWVxK78");
            Multiaddr[] addrs = closestPeers.get(0).addresses.stream().map(a -> new Multiaddr(a.toString())).toArray(Multiaddr[]::new);

            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                    "/peergos/v0/core/getUsernamesGzip/");
            long totalTime = 0;
            int count = 20;
            for (int i = 0; i < count; i++) {
                HttpProtocol.HttpController proxier = new HttpProtocol.Binding(unusedProxyTarget)
                        .dial(node1, PeerId.fromBase58(peerId), addrs)
                        .getController()
                        .orTimeout(10, TimeUnit.SECONDS).join();
                long t1 = System.currentTimeMillis();
                FullHttpResponse resp = proxier.send(httpRequest.retain())
                        .orTimeout(15, TimeUnit.SECONDS).join();
                long t2 = System.currentTimeMillis();
                System.out.println("P2P HTTP request took " + (t2 - t1) + "ms");
                totalTime += t2 - t1;

                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                resp.content().readBytes(bout, resp.headers().getInt("content-length"));
                resp.release();

                GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(bout.toByteArray()));

                List<String> reply = (List)JSONParser.parse(new String(readFully(gzip)));
                Assert.assertTrue(reply.contains("peergos"));
            }
            System.out.println("Average: " + totalTime / count);
        } finally {
            node1.stop();
        }
    }

    public static byte[] readFully(InputStream in) throws IOException {
        ByteArrayOutputStream bout =  new ByteArrayOutputStream();
        byte[] b =  new  byte[0x1000];
        int nRead;
        while ((nRead = in.read(b, 0, b.length)) != -1 )
            bout.write(b, 0, nRead);
        in.close();
        return bout.toByteArray();
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
