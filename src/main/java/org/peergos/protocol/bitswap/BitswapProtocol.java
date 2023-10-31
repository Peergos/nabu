package org.peergos.protocol.bitswap;

import io.libp2p.core.*;
import io.libp2p.protocol.*;
import io.prometheus.client.*;
import org.jetbrains.annotations.*;
import org.peergos.protocol.bitswap.pb.*;
import org.peergos.util.Logging;

import java.util.concurrent.*;
import java.util.logging.*;

public class BitswapProtocol extends ProtobufProtocolHandler<BitswapController> {

    private static final Logger LOG = Logging.LOG();
    private static final Counter initiatorReceivedBytes = Counter.build()
            .name("bitswap_initiator_received_bytes")
            .help("Total received bytes in bitswap initiator")
            .register();
    private static final Counter initiatorSentBytes = Counter.build()
            .name("bitswap_initiator_sent_bytes")
            .help("Total sent bytes in bitswap initiator")
            .register();
    private static final Counter responderReceivedBytes = Counter.build()
            .name("bitswap_responder_received_bytes")
            .help("Total received bytes in bitswap responder")
            .register();
    private static final Counter responderSentBytes = Counter.build()
            .name("bitswap_responder_sent_bytes")
            .help("Total sent bytes in bitswap responder")
            .register();
    private static final Counter blockedConnections = Counter.build()
            .name("bitswap_blocked_connections")
            .help("Total connection attempts blocked in bitswap")
            .register();
    private final BitswapEngine engine;

    public BitswapProtocol(BitswapEngine engine) {
        super(MessageOuterClass.Message.getDefaultInstance(), engine.maxMessageSize(), engine.maxMessageSize());
        this.engine = engine;
    }

    @NotNull
    @Override
    protected CompletableFuture<BitswapController> onStartInitiator(@NotNull Stream stream) {
        BitswapConnection conn = new BitswapConnection(stream, initiatorSentBytes);
        engine.addConnection(stream.remotePeerId(), stream.getConnection().remoteAddress());
        stream.pushHandler(new MessageHandler(engine, initiatorSentBytes, initiatorReceivedBytes));
        return CompletableFuture.completedFuture(conn);
    }

    @NotNull
    @Override
    protected CompletableFuture<BitswapController> onStartResponder(@NotNull Stream stream) {
        if (! engine.allowConnection(stream.remotePeerId())) {
            stream.close();
            blockedConnections.inc();
            return new CompletableFuture<>();
        }
        BitswapConnection conn = new BitswapConnection(stream, responderSentBytes);
        engine.addConnection(stream.remotePeerId(), stream.getConnection().remoteAddress());
        stream.pushHandler(new MessageHandler(engine, responderSentBytes, responderReceivedBytes));
        return CompletableFuture.completedFuture(conn);
    }

    class MessageHandler implements ProtocolMessageHandler<MessageOuterClass.Message> {
        private BitswapEngine engine;
        private final Counter sentBytes, receivedBytes;

        public MessageHandler(BitswapEngine engine, Counter sentBytes, Counter receivedBytes) {
            this.engine = engine;
            this.sentBytes = sentBytes;
            this.receivedBytes = receivedBytes;
        }

        @Override
        public void onMessage(@NotNull Stream stream, MessageOuterClass.Message msg) {
            receivedBytes.inc(msg.getSerializedSize());
            engine.receiveMessage(msg, stream, sentBytes);
        }
    }
}
