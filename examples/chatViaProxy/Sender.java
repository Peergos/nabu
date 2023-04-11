package chatViaProxy;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.HostBuilder;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.HttpUtil;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

public class Sender {
    public Sender(Multiaddr address2) throws IOException {

        InetSocketAddress unusedProxyTarget = new InetSocketAddress("127.0.0.1", 7000);
        HostBuilder builder1 = HostBuilder.build(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(unusedProxyTarget));
        Host node1 = builder1.build();
        node1.start().join();

        int port = 9000;
        InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", port);
        HttpServer localhostServer = HttpServer.create(proxyTarget, 20);
        HttpProtocol.HttpController proxier = new HttpProtocol.Binding(unusedProxyTarget).dial(node1, address2)
                .getController().join();
        localhostServer.createContext("/", new SenderHandler(proxier));
        localhostServer.setExecutor(Executors.newSingleThreadExecutor());
        localhostServer.start();
        System.out.println("Started Sender on port:" + port);
        Thread shutdownHook = new Thread(() -> {
            try {
                node1.stop();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public class SenderHandler implements HttpHandler {
        private HttpProtocol.HttpController proxier;
        public SenderHandler(HttpProtocol.HttpController proxier) {
            this.proxier = proxier;
        }
        @Override
        public void handle(HttpExchange httpExchange) {
            try {

                String path = httpExchange.getRequestURI().getPath();
                HttpMethod method = HttpMethod.valueOf(httpExchange.getRequestMethod());
                byte[] body = read(httpExchange.getRequestBody());
                FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, Unpooled.copiedBuffer(body));
                FullHttpResponse resp = proxier.send(httpRequest.retain()).join();
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                int contentLength = resp.headers().getInt("content-length");
                resp.content().readBytes(bout, contentLength);
                Headers headers = httpExchange.getResponseHeaders();
                for (Map.Entry<String, String> entry: resp.headers().entries()) {
                    headers.replace(entry.getKey(), List.of(entry.getValue()));
                }
                httpExchange.sendResponseHeaders(200, contentLength);
                httpExchange.getResponseBody().write(bout.toByteArray());
            } catch (Exception e) {
                HttpUtil.replyError(httpExchange, e);
            } finally {
                httpExchange.close();
            }
        }
    }

    protected static byte[] read(InputStream in) throws IOException {
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
             OutputStream gout = new DataOutputStream(bout)) {
            byte[] tmp = new byte[4096];
            int r;
            while ((r = in.read(tmp)) >= 0)
                gout.write(tmp, 0, r);
            in.close();
            return bout.toByteArray();
        }
    }

    public static void main(String[] args) throws IOException {
        String addr2 = "/ip4/127.0.0.1/tcp/20595/p2p/12D3KooW9zFPEDNuRVQGEo94inS2uyQNpAVgTyTY25u4nHg4sXKj";
        Multiaddr address2 = Multiaddr.fromString(addr2);
        new Sender(address2);
    }
}
