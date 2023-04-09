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

    public static class Receiver implements ProtocolMessageHandler<HttpRequest>, HttpController {
        private final SocketAddress proxyTarget;

        public Receiver(SocketAddress proxyTarget) {
            this.proxyTarget = proxyTarget;
        }

        private void sendReply(HttpObject reply, Stream p2pstream) {
            if (reply instanceof HttpContent)
                p2pstream.writeAndFlush(((HttpContent) reply).retain());
            else
                p2pstream.writeAndFlush(reply);
        }

        @Override
        public void onMessage(@NotNull Stream stream, HttpRequest msg) {
            Bootstrap b = new Bootstrap();
            b.group(stream.eventLoop())
                    .channel(NioSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.TRACE));

            ChannelFuture fut = b.connect(proxyTarget);
            Channel ch = fut.channel();
            ch.pipeline().addLast(new HttpRequestEncoder());
            ch.pipeline().addLast(new HttpResponseDecoder());
            ch.pipeline().addLast(new ResponseWriter(reply -> sendReply(reply, stream)));

            fut.addListener(x -> ch.writeAndFlush(msg));
        }

        public CompletableFuture<FullHttpResponse> send(FullHttpRequest req) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send form a receiver!"));
        }
    }

    private static final int TRAFFIC_LIMIT = 2*1024*1024;
    private final SocketAddress proxyTarget;

    public HttpProtocol(SocketAddress proxyTarget) {
        super(TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.proxyTarget = proxyTarget;
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
        Receiver proxier = new Receiver(proxyTarget);
        stream.pushHandler(new HttpRequestDecoder());
        stream.pushHandler(proxier);
        stream.pushHandler(new HttpResponseEncoder());
        return CompletableFuture.completedFuture(proxier);
    }
}
