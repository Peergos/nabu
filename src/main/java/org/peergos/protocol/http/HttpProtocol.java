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
        private final Stream stream;

        public ResponseWriter(Stream stream) {
            this.stream = stream;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, HttpObject reply) throws Exception {
            System.out.println("ResponseWriter: " + reply);
            if (reply instanceof HttpContent)
                stream.writeAndFlush(((HttpContent) reply).copy());
            else
                stream.writeAndFlush(reply);
        }
    }

    public static class Receiver implements ProtocolMessageHandler<HttpRequest>, HttpController {
        private final SocketAddress proxyTarget;
        private final Stream p2pstream;

        public Receiver(SocketAddress proxyTarget, Stream p2pstream) {
            this.proxyTarget = proxyTarget;
            this.p2pstream = p2pstream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, HttpRequest msg) {
            System.out.println("Receiver got message");
            Bootstrap b = new Bootstrap();
            b.group(stream.eventLoop())
                    .channel(NioSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO));

            ChannelFuture fut = b.connect(proxyTarget);
            Channel ch = fut.channel();
            ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
            ch.pipeline().addLast(new HttpRequestEncoder());
            ch.pipeline().addLast(new HttpResponseDecoder());
            ch.pipeline().addLast(new ResponseWriter(p2pstream));

            ch.writeAndFlush(msg);
            System.out.println("Receiver forwarded message");
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
        Receiver proxier = new Receiver(proxyTarget, stream);
        stream.pushHandler(new HttpRequestDecoder());
        stream.pushHandler(proxier);
        stream.pushHandler(new HttpResponseEncoder());
        return CompletableFuture.completedFuture(proxier);
    }
}
