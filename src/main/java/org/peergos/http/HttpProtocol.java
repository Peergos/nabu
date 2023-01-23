package org.peergos.http;

import io.libp2p.core.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.embedded.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.proxy.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.concurrent.*;

public class HttpProtocol extends ProtocolHandler<HttpProtocol.HttpController> {

    public static class Binding extends StrictProtocolBinding<HttpController> {
        public Binding(SocketAddress proxyTarget) {
            super("/http", new HttpProtocol(proxyTarget));
        }
    }

    public static class HttpController implements ProtocolMessageHandler<ByteBuf>{
        private final Stream stream;
        private final LinkedBlockingDeque<CompletableFuture<FullHttpResponse>> queue = new LinkedBlockingDeque<>();

        public HttpController(Stream stream) {
            this.stream = stream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, ByteBuf msg) {
            EmbeddedChannel ch = new EmbeddedChannel();
            ChannelPipeline p = ch.pipeline();
            p.addLast(new HttpClientCodec());
            p.addLast(new HttpContentDecompressor());
            p.addLast(new HttpObjectAggregator(10_485_760));

            ch.write(msg);
            Object resp = ch.inboundMessages().poll();
            queue.poll().complete((FullHttpResponse) resp);
        }

        public CompletableFuture<FullHttpResponse> send(FullHttpRequest req) {
            CompletableFuture<FullHttpResponse> res = new CompletableFuture<>();
            queue.add(res);
            ByteBuf byteBuf = Unpooled.buffer();
            req.content().clear().writeBytes(byteBuf);
            stream.writeAndFlush(byteBuf.array());
            return res;
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
        HttpController replyPropagator = new HttpController(stream);
        stream.pushHandler(replyPropagator);
        return CompletableFuture.completedFuture(replyPropagator);
    }

    @NotNull
    @Override
    protected CompletableFuture<HttpController> onStartResponder(@NotNull Stream stream) {
        HttpController replyPropagator = new HttpController(stream);
        stream.pushHandler(new HttpRequestDecoder());
        stream.pushHandler(new HttpProxyHandler(proxyTarget));
        stream.pushHandler(new HttpResponseEncoder());
        return CompletableFuture.completedFuture(replyPropagator);
    }
}
