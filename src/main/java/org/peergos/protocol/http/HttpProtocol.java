package org.peergos.protocol.http;

import io.libp2p.core.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import io.netty.bootstrap.*;
import io.netty.channel.*;
import io.netty.channel.nio.*;
import io.netty.channel.socket.nio.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.logging.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.concurrent.*;
import java.util.function.*;

public class HttpProtocol extends ProtocolHandler<HttpProtocol.HttpController> {

    public static class Binding extends StrictProtocolBinding<HttpController> {
        public Binding(SocketAddress proxyTarget) {
            super("/http", new HttpProtocol(proxyTarget));
        }

        public Binding(HttpRequestProcessor handler) {
            super("/http", new HttpProtocol(handler));
        }
    }

    public interface HttpController {
        CompletableFuture<FullHttpResponse> send(FullHttpRequest req);
    }

    public static class Sender implements ProtocolMessageHandler<FullHttpResponse>, HttpController {
        private final Stream stream;
        private final LinkedBlockingDeque<CompletableFuture<FullHttpResponse>> queue = new LinkedBlockingDeque<>();

        public Sender(Stream stream) {
            this.stream = stream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, FullHttpResponse msg) {
            queue.poll().complete(msg.copy());
        }

        public CompletableFuture<FullHttpResponse> send(FullHttpRequest req) {
            CompletableFuture<FullHttpResponse> res = new CompletableFuture<>();
            queue.add(res);
            req.headers().set(HttpHeaderNames.HOST, stream.remotePeerId());
            stream.writeAndFlush(req);
            return res;
        }
    }

    public static class ResponseWriter extends SimpleChannelInboundHandler<HttpObject> {
        private final Consumer<HttpObject> replyProcessor;

        public ResponseWriter(Consumer<HttpObject> replyProcessor) {
            this.replyProcessor = replyProcessor;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, HttpObject reply) {
            replyProcessor.accept(reply);
        }
    }

    public interface HttpRequestProcessor {
        void handle(Stream stream, HttpRequest msg, Consumer<HttpObject> replyHandler);
    }

    public static class Receiver implements ProtocolMessageHandler<HttpRequest>, HttpController {
        private final HttpRequestProcessor requestHandler;

        public Receiver(HttpRequestProcessor requestHandler) {
            this.requestHandler = requestHandler;
        }

        private void sendReply(HttpObject reply, Stream p2pstream) {
            if (reply instanceof HttpContent)
                p2pstream.writeAndFlush(((HttpContent) reply).retain());
            else
                p2pstream.writeAndFlush(reply);
        }

        @Override
        public void onMessage(@NotNull Stream stream, HttpRequest msg) {
            requestHandler.handle(stream, msg, reply -> sendReply(reply, stream));
        }

        public CompletableFuture<FullHttpResponse> send(FullHttpRequest req) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send form a receiver!"));
        }
    }

    public static void proxyRequest(Stream stream,
                                    HttpRequest msg,
                                    SocketAddress proxyTarget,
                                    Consumer<HttpObject> replyHandler) {
        Bootstrap b = new Bootstrap();
        b.group(stream.eventLoop())
                .channel(NioSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.TRACE));

        ChannelFuture fut = b.connect(proxyTarget);
        Channel ch = fut.channel();
        ch.pipeline().addLast(new HttpRequestEncoder());
        ch.pipeline().addLast(new HttpResponseDecoder());
        ch.pipeline().addLast(new ResponseWriter(replyHandler));

        fut.addListener(x -> ch.writeAndFlush(msg));
    }

    private static final int TRAFFIC_LIMIT = 2*1024*1024;
    private final HttpRequestProcessor handler;

    public HttpProtocol(HttpRequestProcessor handler) {
        super(TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.handler = (s, req, replyHandler) -> handler.handle(s, setHost(req, s), replyHandler);
    }

    public static HttpRequest setHost(HttpRequest req, Stream source) {
        req.headers().set(HttpHeaderNames.HOST,source.remotePeerId().toBase58());
        return req;
    }

    public HttpProtocol(SocketAddress proxyTarget) {
        this((s, req, replyHandler) -> proxyRequest(s, setHost(req, s), proxyTarget, replyHandler));
    }

    @NotNull
    @Override
    protected CompletableFuture<HttpController> onStartInitiator(@NotNull Stream stream) {
        Sender replyPropagator = new Sender(stream);
        stream.pushHandler(new HttpRequestEncoder());
        stream.pushHandler(new HttpResponseDecoder());
        stream.pushHandler(new HttpObjectAggregator(1024*1024));
        stream.pushHandler(replyPropagator);
        return CompletableFuture.completedFuture(replyPropagator);
    }

    @NotNull
    @Override
    protected CompletableFuture<HttpController> onStartResponder(@NotNull Stream stream) {
        Receiver proxier = new Receiver(handler);
        stream.pushHandler(new HttpRequestDecoder());
        stream.pushHandler(new HttpObjectAggregator(2*1024*1024));
        stream.pushHandler(proxier);
        stream.pushHandler(new HttpResponseEncoder());
        return CompletableFuture.completedFuture(proxier);
    }
}
