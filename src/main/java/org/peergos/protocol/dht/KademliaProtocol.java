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
        super(Dht.Message.getDefaultInstance().getDefaultInstance(), MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE);
        this.engine = engine;
    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartInitiator(@NotNull Stream stream) {
        KademliaConnection conn = new KademliaConnection(stream);
        engine.addOutgoingConnection(stream.remotePeerId(), conn, stream.getConnection().remoteAddress());
        stream.pushHandler(new ReplyHandler(engine));
        return CompletableFuture.completedFuture(conn);
    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartResponder(@NotNull Stream stream) {
        KademliaConnection conn = new KademliaConnection(stream);
        engine.addIncomingConnection(stream.remotePeerId(), conn, stream.getConnection().remoteAddress());
        stream.pushHandler(new IncomingRequestHandler(engine, stream));
        return CompletableFuture.completedFuture(conn);
    }

    class ReplyHandler implements ProtocolMessageHandler<Dht.Message> {
        private KademliaEngine engine;

        public ReplyHandler(KademliaEngine engine) {
            this.engine = engine;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            engine.receiveReply(msg, stream.remotePeerId());
        }
    }

    class IncomingRequestHandler implements ProtocolMessageHandler<Dht.Message> {
        private final KademliaEngine engine;
        private final Stream stream;

        public IncomingRequestHandler(KademliaEngine engine, Stream stream) {
            this.engine = engine;
            this.stream = stream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            engine.receiveRequest(msg, stream.remotePeerId(), stream);
        }
    }
}
