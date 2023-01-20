package org.peergos.bitswap;

import bitswap.message.pb.*;
import io.libp2p.core.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.function.*;

public class BitswapProtocol extends ProtobufProtocolHandler<BitswapController> {

    private final BitswapEngine engine;

    public BitswapProtocol(BitswapEngine engine) {
        super(MessageOuterClass.Message.getDefaultInstance(), Bitswap.MAX_MESSAGE_SIZE, Bitswap.MAX_MESSAGE_SIZE);
        this.engine = engine;
    }

    @NotNull
    @Override
    protected CompletableFuture<BitswapController> onStartInitiator(@NotNull Stream stream) {
        BitswapConnection conn = new BitswapConnection(stream);
        engine.addConnection(stream.remotePeerId(), conn);
        stream.pushHandler(new MessageHandler(engine));
        return CompletableFuture.completedFuture(conn);
    }

    @NotNull
    @Override
    protected CompletableFuture<BitswapController> onStartResponder(@NotNull Stream stream) {
        BitswapConnection conn = new BitswapConnection(stream);
        engine.addConnection(stream.remotePeerId(), conn);
        stream.pushHandler(new MessageHandler(engine));
        return CompletableFuture.completedFuture(conn);
    }

    class MessageHandler implements ProtocolMessageHandler<MessageOuterClass.Message> {
        private BitswapEngine engine;

        public MessageHandler(BitswapEngine engine) {
            this.engine = engine;
        }

        @Override
        public void onMessage(@NotNull Stream stream, MessageOuterClass.Message msg) {
            engine.receiveMessage(msg, stream.remotePeerId());
        }
    }
}
