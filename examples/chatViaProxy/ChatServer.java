package chatViaProxy;

import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpUtil;

import org.peergos.HostBuilder;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.JSONParser;

import java.io.*;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;

import static java.util.UUID.randomUUID;

public class ChatServer {
    public ChatServer() throws Exception {

        Chat chat = new Chat();

        HttpProtocol.Binding node2Http = new HttpProtocol.Binding((s, req, h) -> {
            System.out.println("Node 2 received: " + req);
        });
        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(node2Http);
        Host node2 = builder2.build();
        node2.start().join();
        Multiaddr address2 = node2.listenAddresses().get(0);
        System.out.println("Running Multiaddr: " + address2.toString());

        int port = 8000;
        new HttpServer(chat, port).run();

        System.out.println("Started Chat server on port:" + port);
        Thread shutdownHook = new Thread(() -> {
            try {
                node2.stop();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
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
        public byte[] getChatPage()  {
            try {
                return read(ChatServer.class.getResourceAsStream("assets/index.html"));
            } catch (IOException ioe) {
                ioe.printStackTrace();
                return "".getBytes();
            }
        }
        private byte[] read(InputStream in) throws IOException {
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
        public Message addMessage(String body) {
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
    // tutorial https://www.baeldung.com/java-netty-http-server
    // and accompanying code from https://github.com/eugenp/tutorials/tree/master/server-modules/netty/src/main/java/com/baeldung/http/server
    public class HttpServer {
        private int port;
        private Chat chat;
        public HttpServer(Chat chat, int port) {
            this.chat = chat;
            this.port = port;
        }
        public void run() throws Exception {
            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .handler(new LoggingHandler(LogLevel.TRACE))
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ChannelPipeline p = ch.pipeline();
                                p.addLast(new HttpRequestDecoder());
                                p.addLast(new HttpResponseEncoder());
                                p.addLast(new CustomHttpServerHandler(chat));
                            }
                        });

                ChannelFuture f = b.bind(port).sync();
                f.channel().closeFuture().sync();
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }
    }
    public class CustomHttpServerHandler extends SimpleChannelInboundHandler<Object> {
        private final Chat chat;
        private HttpMethod method;
        private String uri;
        private boolean keepAlive;

        private StringBuilder responseData;
        private HttpResponseStatus responseCode;
        private Map<String, String> responseHeaders;
        private String mimeType;
        public CustomHttpServerHandler(Chat chat) {
            this.chat = chat;
        }
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        //NOTE: is called twice for each request!
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msgObj) {
            if (msgObj instanceof HttpRequest) {
                HttpRequest request = (HttpRequest) msgObj;
                this.method = request.method();
                this.uri = request.uri();
                this.keepAlive = HttpUtil.isKeepAlive(request);

                responseData = new StringBuilder();
                responseCode = HttpResponseStatus.OK;
                responseHeaders = new HashMap<>();
                mimeType = "text/plain";
            } else if (msgObj instanceof HttpContent) {
                HttpContent httpContent = (HttpContent) msgObj;
                int paramIndex = uri.lastIndexOf('?');
                String path =  paramIndex > -1 ? uri.substring(0, paramIndex) : uri;
                switch (path) {
                    case "/": {
                        if (method == HttpMethod.GET) {
                            byte[] data = chat.getChatPage();
                            responseData.append(new String(data));
                            responseCode = HttpResponseStatus.OK;
                            mimeType = "text/html";
                        }
                        break;
                    }
                    case "/messages": {
                        if (method == HttpMethod.GET) {
                            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
                            Map<String, List<String>> params = queryStringDecoder.parameters();
                            Optional<String> fromOpt = Optional.ofNullable(params.get("from")).map(a -> a.get(0));
                            List<Message> list = chat.getMessages(fromOpt);
                            List<Map<String, Object>> msgs = list.stream().map(msg -> msg.toJson()).collect(Collectors.toList());
                            String data = JSONParser.toString(msgs);
                            responseData.append(data);
                            responseCode = HttpResponseStatus.OK;
                            mimeType = "text/plain";
                        }
                        break;
                    }
                    case "/sendMessage": {
                        if (method == HttpMethod.POST) {
                            ByteBuf content = httpContent.content();
                            Message message = chat.addMessage(content.toString(CharsetUtil.UTF_8));
                            String data = JSONParser.toString(message.toJson());
                            responseHeaders.put(HttpHeaderNames.LOCATION.toString(), message.id);
                            responseData.append(data);
                            responseCode = HttpResponseStatus.CREATED;
                            mimeType = "text/plain";
                        }
                        break;
                    }
                    default: {
                        responseCode = BAD_REQUEST;
                        break;
                    }
                }
                if (msgObj instanceof LastHttpContent) {
                    LastHttpContent trailer = (LastHttpContent) msgObj;
                    if (! trailer.decoderResult().isSuccess()) {
                        responseCode = BAD_REQUEST;
                    }
                    writeResponse(ctx);
                }
            }
        }

        private void writeResponse(ChannelHandlerContext ctx) {

            FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, responseCode, Unpooled.copiedBuffer(responseData.toString(), CharsetUtil.UTF_8));

            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeType + "; charset=UTF-8");
            for(Map.Entry<String, String> entry : responseHeaders.entrySet()) {
                httpResponse.headers().set(entry.getKey(), entry.getValue());
            }
            if (keepAlive) {
                httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());
                httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            ctx.write(httpResponse);
            if (!keepAlive) {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
    public static void main(String[] args) throws Exception {
        new ChatServer();
    }
}
