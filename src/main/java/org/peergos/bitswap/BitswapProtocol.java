package org.peergos.bitswap;

import bitswap.message.pb.*;
import io.libp2p.core.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.function.*;

public class BitswapProtocol extends ProtobufProtocolHandler<BitswapController> {

    public BitswapProtocol() {
        super(MessageOuterClass.Message.getDefaultInstance(), Bitswap.MAX_MESSAGE_SIZE, Bitswap.MAX_MESSAGE_SIZE);
    }

    @NotNull
    @Override
    protected CompletableFuture<BitswapController> onStartInitiator(@NotNull Stream stream) {
        BitswapConnection conn = new BitswapConnection(stream);
        stream.pushHandler(new MessageHandler(conn));
        return CompletableFuture.completedFuture(conn);
    }

    @NotNull
    @Override
    protected CompletableFuture<BitswapController> onStartResponder(@NotNull Stream stream) {
        BitswapConnection conn = new BitswapConnection(stream);
        stream.pushHandler(new MessageHandler(conn));
        return CompletableFuture.completedFuture(conn);
    }

    class MessageHandler implements ProtocolMessageHandler<MessageOuterClass.Message> {
        private BitswapConnection conn;

        public MessageHandler(BitswapConnection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(@NotNull Stream stream, MessageOuterClass.Message msg) {
            conn.receive(msg);
        }
    }
}
