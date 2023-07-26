package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.junit.Assert;
import org.junit.Test;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.net.HttpProxyHandler;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.http.HttpProtocol;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class P2pHttpTest {

    @Test
    public void p2pTest() {

        HttpProtocol.Binding node1Http = new HttpProtocol.Binding((s, req, h) -> {
            FullHttpResponse emptyReply = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.buffer(0));
            emptyReply.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            h.accept(emptyReply.retain());
        });
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true));
        builder1 = builder1.addProtocol(node1Http);
        Host node1 = builder1.build();
        node1.start().join();

        String responseText = "nabu!";
        String requestBody = "request body!";
        Map<String, String> requestHeaders = new HashMap<>();
        String testHeaderKey = "testProp";
        String testHeaderValue = "testPropValue";
        requestHeaders.put(testHeaderKey, testHeaderValue);
        String urlParamKey = "text";
        String urlParamValue = "hello";
        String urlParam = "?" + urlParamKey + "=" + urlParamValue + "";
        RamBlockstore blockstore2 = new RamBlockstore();
        int localPort = TestPorts.getPort();

        HttpProtocol.Binding node2Http = new HttpProtocol.Binding((s, req, h) -> {
            System.out.println("Node 2 received: " + req);
            printBody(req);
            FullHttpRequest fullRequest = req;
            String uri = req.uri();
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
            Map<String, List<String>> params = queryStringDecoder.parameters();
            String paramValue = params.get(urlParamKey).get(0);
            Assert.assertTrue("request url param", paramValue.equals(urlParamValue));
            HttpHeaders headers = req.headers();
            String headerValue = headers.get(testHeaderKey);
            Assert.assertTrue("request header", headerValue.equals(testHeaderValue));

            ByteBuf content = fullRequest.content();
            String bodyContent = content.toString(CharsetUtil.UTF_8);
            Assert.assertTrue("body content", requestBody.equals(bodyContent));
            HttpProtocol.proxyRequest(req, new InetSocketAddress("127.0.0.1", localPort), h);
        });
        HostBuilder builder2 = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(), new RamRecordStore(), blockstore2, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        builder2 = builder2.addProtocol(node2Http);
        Host node2 = builder2.build();
        node2.start().join();

        Multiaddr node2Address = node2.listenAddresses().get(0);
        PeerId peerId2 = node2Address.getPeerId();
        Multihash multihash2 = Multihash.fromBase58(peerId2.toBase58());
        Cid peerAsCid = new Cid(1, Cid.Codec.Libp2pKey, multihash2.getType(), multihash2.getHash());
        node1.getAddressBook().setAddrs(peerId2, 0, node2Address).join();

        HttpServer apiServer1 = null;
        HttpServer server2 = null;
        try {
            int port = 8777;
            MultiAddress apiAddress1 = new MultiAddress("/ip4/127.0.0.1/tcp/" + port);
            InetSocketAddress localAPIAddress1 = new InetSocketAddress(apiAddress1.getHost(), apiAddress1.getPort());
            apiServer1 = HttpServer.create(localAPIAddress1, 500);
            apiServer1.createContext(HttpProxyService.API_URL, new HttpProxyHandler(new HttpProxyService(node1, node1Http, null)));
            apiServer1.setExecutor(Executors.newFixedThreadPool(50));
            apiServer1.start();

            InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", localPort);
            server2 = HttpServer.create(proxyTarget, 20);
            server2.createContext("/", httpExchange -> {
                try {
                    byte[] body = responseText.getBytes();
                    httpExchange.sendResponseHeaders(200, body.length);
                    httpExchange.getResponseBody().write(body);
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.assertTrue(false);
                } finally {
                    httpExchange.close();
                }
            });
            server2.setExecutor(Executors.newSingleThreadExecutor());
            server2.start();

            URL target = new URL("http", "localhost", port,
                    "/p2p/" + peerAsCid + "/http/message" + urlParam);
            Response reply = send(target, "POST", requestBody.getBytes(), requestHeaders);
            Assert.assertTrue("reply", responseText.equals(new String(reply.body)));
        } catch (IOException ioe) {
            ioe.printStackTrace();
            Assert.assertTrue("IOException", false);
        } finally {
            node1.stop();
            node2.stop();
            if (apiServer1 != null) {
                apiServer1.stop(1);
            }
            if (server2 != null) {
                server2.stop(1);
            }
        }
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

    public static void printBody(HttpRequest req) {
        if (req instanceof FullHttpRequest) {
            ByteBuf content = ((FullHttpRequest) req).content();
            System.out.println(content.getCharSequence(0, content.readableBytes(), Charset.defaultCharset()));
        }

    }
}
