package org.peergos.protocol.http;

import io.ipfs.cid.*;
import io.ipfs.multibase.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import io.netty.bootstrap.*;
import io.netty.channel.*;
import io.netty.channel.nio.*;
import io.netty.channel.socket.nio.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.concurrent.*;
import java.util.function.*;

public class HttpProtocol extends ProtocolHandler<HttpProtocol.HttpController> {
    private static final int MAX_BODY_SIZE = 2 * 1024 * 1024; // TODO remove this and make it fully streaming

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
            CompletableFuture<FullHttpResponse> req = queue.poll();
            if (req != null)
                req.complete(msg.copy());
            msg.release();
            stream.close();
        }

        public CompletableFuture<FullHttpResponse> send(FullHttpRequest req) {
            CompletableFuture<FullHttpResponse> res = new CompletableFuture<>();
            queue.add(res);
            FullHttpRequest withTargetHost = setHost(req, stream.remotePeerId());
            stream.writeAndFlush(withTargetHost);
            stream.closeWrite();
            return res;
        }
    }

    public static class ResponseWriter extends SimpleChannelInboundHandler<HttpContent> {
        private final Consumer<HttpContent> replyProcessor;

        public ResponseWriter(Consumer<HttpContent> replyProcessor) {
            this.replyProcessor = replyProcessor;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext channelHandlerContext, HttpContent reply) {
            replyProcessor.accept(reply);
        }
    }

    public interface HttpRequestProcessor {
        void handle(Stream stream, FullHttpRequest msg, Consumer<HttpContent> replyHandler);
    }

    public static class Receiver implements ProtocolMessageHandler<FullHttpRequest>, HttpController {
        private final HttpRequestProcessor requestHandler;

        public Receiver(HttpRequestProcessor requestHandler) {
            this.requestHandler = requestHandler;
        }

        private void sendReply(HttpContent reply, Stream p2pstream) {
            p2pstream.writeAndFlush(reply.copy());
        }

        @Override
        public void onMessage(@NotNull Stream stream, FullHttpRequest msg) {
            requestHandler.handle(stream, msg, reply -> sendReply(reply, stream));
        }

        public CompletableFuture<FullHttpResponse> send(FullHttpRequest req) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send from a receiver!"));
        }
    }

    private static final NioEventLoopGroup pool = new NioEventLoopGroup();
    public static void proxyRequest(FullHttpRequest msg,
                                    SocketAddress proxyTarget,
                                    Consumer<HttpContent> replyHandler) {
        Bootstrap b = new Bootstrap()
                .group(pool)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(new HttpRequestEncoder());
                        ch.pipeline().addLast(new HttpResponseDecoder());
                        ch.pipeline().addLast(new HttpObjectAggregator(MAX_BODY_SIZE));
                        ch.pipeline().addLast(new ResponseWriter(replyHandler));
                    }
                });

        ChannelFuture fut = b.connect(proxyTarget);
        Channel ch = fut.channel();

        FullHttpRequest retained = msg.retain();
        fut.addListener(x -> ch.writeAndFlush(retained));
    }

    private static final long TRAFFIC_LIMIT = Long.MAX_VALUE; // This is the total inbound or outbound traffic allowed, not a rate
    private final HttpRequestProcessor handler;

    public HttpProtocol(HttpRequestProcessor handler) {
        super(TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.handler = handler;
    }

    public static FullHttpRequest setHost(FullHttpRequest req, PeerId us) {
        req.headers().set(HttpHeaderNames.HOST, us.toBase58());
        return req;
    }

    public HttpProtocol(SocketAddress proxyTarget) {
        this((s, req, replyHandler) -> proxyRequest(req, proxyTarget, replyHandler));
    }

    @NotNull
    @Override
    protected CompletableFuture<HttpController> onStartInitiator(@NotNull Stream stream) {
        Sender replyPropagator = new Sender(stream);
        stream.pushHandler(new HttpRequestEncoder());
        stream.pushHandler(new HttpResponseDecoder());
        stream.pushHandler(new HttpObjectAggregator(MAX_BODY_SIZE));
        stream.pushHandler(replyPropagator);
        stream.pushHandler(new LoggingHandler(LogLevel.TRACE));
        return CompletableFuture.completedFuture(replyPropagator);
    }

    @NotNull
    @Override
    protected CompletableFuture<HttpController> onStartResponder(@NotNull Stream stream) {
        Receiver proxier = new Receiver(handler);
        stream.pushHandler(new HttpRequestDecoder());
        stream.pushHandler(new HttpObjectAggregator(MAX_BODY_SIZE));
        stream.pushHandler(proxier);
        stream.pushHandler(new HttpResponseEncoder());
        return CompletableFuture.completedFuture(proxier);
    }
}
