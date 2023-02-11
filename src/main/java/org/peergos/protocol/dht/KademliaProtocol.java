package org.peergos.protocol.dht;

import io.libp2p.core.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;
import org.peergos.protocol.dht.pb.Dht;

import java.util.concurrent.*;

public class KademliaProtocol extends ProtobufProtocolHandler<KademliaController> {
    public static final int MAX_MESSAGE_SIZE = 1024*1024;

    private final KademliaEngine engine;

    public KademliaProtocol(KademliaEngine engine) {
        super(Dht.Message.getDefaultInstance(), MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE);
        this.engine = engine;
    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartInitiator(@NotNull Stream stream) {
        engine.addOutgoingConnection(stream.remotePeerId(), stream.getConnection().remoteAddress());
        ReplyHandler handler = new ReplyHandler(stream);
        stream.pushHandler(handler);
        return CompletableFuture.completedFuture(handler);
    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartResponder(@NotNull Stream stream) {
        engine.addIncomingConnection(stream.remotePeerId(), stream.getConnection().remoteAddress());
        IncomingRequestHandler handler = new IncomingRequestHandler(engine);
        stream.pushHandler(handler);
        return CompletableFuture.completedFuture(handler);
    }

    class ReplyHandler implements ProtocolMessageHandler<Dht.Message>, KademliaController {
        private final CompletableFuture<Dht.Message> resp = new CompletableFuture<>();
        private final Stream stream;

        public ReplyHandler(Stream stream) {
            this.stream = stream;
        }

        @Override
        public CompletableFuture<Dht.Message> rpc(Dht.Message msg) {
            stream.writeAndFlush(msg);
            return resp;
        }

        @Override
        public CompletableFuture<Boolean> send(Dht.Message msg) {
            stream.writeAndFlush(msg);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            resp.complete(msg);
            stream.closeWrite();
        }

        @Override
        public void onClosed(@NotNull Stream stream) {
            resp.completeExceptionally(new ConnectionClosedException());
        }

        @Override
        public void onException(@Nullable Throwable cause) {
            resp.completeExceptionally(cause);
        }
    }

    class IncomingRequestHandler implements ProtocolMessageHandler<Dht.Message>, KademliaController {
        private final KademliaEngine engine;

        public IncomingRequestHandler(KademliaEngine engine) {
            this.engine = engine;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            engine.receiveRequest(msg, stream.remotePeerId(), stream);
        }

        @Override
        public CompletableFuture<Dht.Message> rpc(Dht.Message msg) {
            throw new IllegalStateException("Responder only!");
        }

        @Override
        public CompletableFuture<Boolean> send(Dht.Message msg) {
            throw new IllegalStateException("Responder only!");
        }
    }
}
