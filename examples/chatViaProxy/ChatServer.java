package chatViaProxy;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.buffer.ByteBuf;
import org.peergos.HostBuilder;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.HttpUtil;
import org.peergos.util.JSONParser;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.UUID.randomUUID;

public class ChatServer {
    public ChatServer() throws IOException {

        Chat chat = new Chat();

        HttpProtocol.Binding node2Http = new HttpProtocol.Binding((s, req, h) -> {
            System.out.println("Node 2 received: " + req);
            String path = req.uri();
            try {
                switch (path) {
                    case "/": {
                        if (req.method() == HttpMethod.GET) {
                            byte[] data = chat.getChatPage();
                            FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(new String(data), CharsetUtil.UTF_8));
                            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
                            h.accept(resp);
                        }
                        break;
                    }
                    case "/sendMessage": {
                        if (req.method() == HttpMethod.POST) {
                            HttpContent httpContent = (HttpContent) req;
                            StringBuilder responseData = new StringBuilder();
                            ByteBuf content = httpContent.content();
                            responseData.append(content.toString(CharsetUtil.UTF_8));
                            Message message = chat.addMessage(responseData.toString().getBytes());
                            String data = JSONParser.toString(message.toJson());
                            FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED, Unpooled.copiedBuffer(data, CharsetUtil.UTF_8));
                            resp.headers().set(HttpHeaderNames.LOCATION, message.id);
                            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                            h.accept(resp);
                        }
                        break;
                    }
                    case "/messages": {
                        if (req.method() == HttpMethod.GET) {
                            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(path);
                            Map<String, List<String>> params = queryStringDecoder.parameters();
                            Optional<String> fromOpt = Optional.ofNullable(params.get("from")).map(a -> a.get(0));
                            List<Message> list = chat.getMessages(fromOpt);
                            List<Map<String, Object>> msgs = list.stream().map(msg -> msg.toJson()).collect(Collectors.toList());
                            String data = JSONParser.toString(msgs);
                            FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(data, CharsetUtil.UTF_8));
                            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                            h.accept(resp);
                        }
                        break;
                    }
                    default: {
                        FullHttpResponse replyErr = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.buffer(0));
                        replyErr.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
                        h.accept(replyErr);
                        break;
                    }
                }
            } catch (Exception e) {
                Throwable cause = e.getCause();
                FullHttpResponse replyException = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, Unpooled.buffer(0));
                try {
                if (cause != null)
                    replyException.headers().set("Trailer", URLEncoder.encode(cause.getMessage(), "UTF-8"));
                else
                    replyException.headers().set("Trailer", URLEncoder.encode(e.getMessage(), "UTF-8"));
                } catch (IOException ex) {
                    replyException.headers().set("Trailer", "Unexpected error");
                }
                replyException.headers().set("Content-Type", "text/plain");
                replyException.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
                h.accept(replyException);
            }
        });
        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(node2Http);
        Host node2 = builder2.build();
        node2.start().join();
        Multiaddr address2 = node2.listenAddresses().get(0);
        System.out.println("Running Multiaddr: " + address2.toString());

        int port = 8000;
        InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", port);
        HttpServer localhostServer = HttpServer.create(proxyTarget, 20);
        localhostServer.createContext("/", new ChatHandler(chat));
        localhostServer.setExecutor(Executors.newSingleThreadExecutor());
        localhostServer.start();
        System.out.println("Started Chat server on port:" + port);
        Thread shutdownHook = new Thread(() -> {
            try {
                localhostServer.stop(1);
                node2.stop();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
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
    private static void replyJson(HttpExchange exchange, String json, int status) {
        try (DataOutputStream dout = new DataOutputStream(exchange.getResponseBody())){
            byte[] raw = json.getBytes();
            exchange.sendResponseHeaders(status, raw.length);
            dout.write(raw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public class Message {
        public final String id;
        public final String text;
        public final String author;
        public final LocalDateTime timestamp;
        public Message(String id, String text, String author, LocalDateTime timestamp) {
            this.id = id;
            this.text = text;
            this.author = author;
            this.timestamp = timestamp;
        }
        public Map<String, Object> toJson() {
            Map<String, Object> res = new HashMap<>();
            res.put("id", id);
            res.put("text", text);
            res.put("author", author);
            res.put("timestamp", timestamp.toString());
            return res;
        }
    }
    public class Chat {
        private List<Message> messages = new ArrayList<>();
        public Chat() {

        }
        public byte[] getChatPage() throws IOException {
            return read(ChatServer.class.getResourceAsStream("assets/index.html"));
        }
        public Message addMessage(byte[] body) {
            Map<String, Object> json = (Map) JSONParser.parse(body);
            String id = randomUUID().toString();
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            Message message = new Message(id, (String)json.get("text"), (String)json.get("author"), now);
            messages.add(message);
            return message;
        }
        public List<Message> getMessages(Optional<String> fromOpt) {
            Integer from = fromOpt.isPresent() ? Integer.parseInt(fromOpt.get()) : 0;
            return messages.stream().skip(from).collect(Collectors.toList());
        }
    }
    public class ChatHandler implements HttpHandler {

        private final Chat chat;
        public ChatHandler(Chat chat) {
            this.chat = chat;
        }
        @Override
        public void handle(HttpExchange httpExchange) {
            String path = httpExchange.getRequestURI().getPath();
            try {
                switch (path) {
                    case "/": {
                        if (httpExchange.getRequestMethod().equals("GET")) {
                            byte[] httpReply = chat.getChatPage();
                            httpExchange.sendResponseHeaders(200, httpReply.length);
                            httpExchange.getResponseBody().write(httpReply);
                        }
                        break;
                    }
                    case "/sendMessage": {
                        if (httpExchange.getRequestMethod().equals("POST")) {
                            Message message = chat.addMessage(read(httpExchange.getRequestBody()));
                            Headers headers = httpExchange.getResponseHeaders();
                            headers.set("location", message.id);
                            replyJson(httpExchange, JSONParser.toString(message.toJson()), 201);
                        }
                        break;
                    }
                    case "/messages": {
                        if (httpExchange.getRequestMethod().equals("GET")) {
                            Map<String, List<String>> params = HttpUtil.parseQuery(httpExchange.getRequestURI().getQuery());
                            Optional<String> fromOpt = Optional.ofNullable(params.get("from")).map(a -> a.get(0));
                            List<Message> list = chat.getMessages(fromOpt);
                            List<Map<String, Object>> msgs = list.stream().map(msg -> msg.toJson()).collect(Collectors.toList());
                            replyJson(httpExchange, JSONParser.toString(msgs), 200);
                        }
                        break;
                    }
                    default: {
                        httpExchange.sendResponseHeaders(404, 0);
                        break;
                    }
                }
            } catch (Exception e) {
                HttpUtil.replyError(httpExchange, e);
            } finally {
                httpExchange.close();
            }
        }
    }
    public static void main(String[] args) throws IOException {
        new ChatServer();
    }
}
