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
        engine.addConnection(stream.remotePeerId(), conn);
        stream.pushHandler(new MessageHandler(engine));
        return CompletableFuture.completedFuture(conn);
    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartResponder(@NotNull Stream stream) {
        KademliaConnection conn = new KademliaConnection(stream);
        engine.addConnection(stream.remotePeerId(), conn);
        stream.pushHandler(new MessageHandler(engine));
        return CompletableFuture.completedFuture(conn);
    }

    class MessageHandler implements ProtocolMessageHandler<Dht.Message> {
        private KademliaEngine engine;

        public MessageHandler(KademliaEngine engine) {
            this.engine = engine;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            engine.receiveMessage(msg, stream.remotePeerId());
        }
    }
}
