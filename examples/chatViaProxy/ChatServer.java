package chatViaProxy;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.peergos.util.HttpUtil;
import org.peergos.util.JSONParser;

import java.io.*;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.UUID.randomUUID;

public class ChatServer {
    public ChatServer() throws IOException {
        int port = 8000;
        InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", port);
        HttpServer localhostServer = HttpServer.create(proxyTarget, 20);
        localhostServer.createContext("/", new ChatHandler());
        localhostServer.setExecutor(Executors.newSingleThreadExecutor());
        localhostServer.start();
        System.out.println("Started Chat server on port:" + port);
        Thread shutdownHook = new Thread(() -> {
            try {
                localhostServer.stop(1);
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
    public class ChatHandler implements HttpHandler {
        private List<Message> messages = new ArrayList<>();
        public ChatHandler() {}
        @Override
        public void handle(HttpExchange httpExchange) {
            String path = httpExchange.getRequestURI().getPath();
            try {
                switch (path) {
                    case "/": {
                        if (httpExchange.getRequestMethod().equals("GET")) {
                            byte[] httpReply = read(ChatServer.class.getResourceAsStream("assets/index.html"));
                            httpExchange.sendResponseHeaders(200, httpReply.length);
                            httpExchange.getResponseBody().write(httpReply);
                        }
                        break;
                    }
                    case "/sendMessage": {
                        if (httpExchange.getRequestMethod().equals("POST")) {
                            String body = new String(read(httpExchange.getRequestBody()));
                            Map<String, Object> json = (Map) JSONParser.parse(body);
                            System.currentTimeMillis();
                            String id = randomUUID().toString();
                            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
                            Message message = new Message(id, (String)json.get("text"), (String)json.get("author"), now);
                            messages.add(message);
                            Headers headers = httpExchange.getResponseHeaders();
                            headers.set("location", id);
                            replyJson(httpExchange, JSONParser.toString(message.toJson()), 201);
                        }
                        break;
                    }
                    case "/messages": {
                        if (httpExchange.getRequestMethod().equals("GET")) {
                            Map<String, List<String>> params = HttpUtil.parseQuery(httpExchange.getRequestURI().getQuery());
                            Optional<String> fromOpt = Optional.ofNullable(params.get("from")).map(a -> a.get(0));
                            Integer from = fromOpt.isPresent() ? Integer.parseInt(fromOpt.get()) : 0;
                            List<Message> list = messages.stream().skip(from).collect(Collectors.toList());
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
        private static void replyJson(HttpExchange exchange, String json, int status) {
            try (DataOutputStream dout = new DataOutputStream(exchange.getResponseBody())){
                byte[] raw = json.getBytes();
                exchange.sendResponseHeaders(status, raw.length);
                dout.write(raw);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {
        new ChatServer();
    }
}
