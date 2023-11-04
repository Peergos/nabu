package org.peergos.protocol.dht;

import io.libp2p.core.*;
import io.libp2p.protocol.*;
import io.prometheus.client.*;
import org.jetbrains.annotations.*;
import org.peergos.protocol.dht.pb.Dht;

import java.util.concurrent.*;

public class KademliaProtocol extends ProtobufProtocolHandler<KademliaController> {

    private static final Counter initiatorReceivedBytes = Counter.build()
            .name("kademlia_initiator_received_bytes")
            .help("Total received bytes in kademlia protocol initiator")
            .register();
    private static final Counter initiatorSentBytes = Counter.build()
            .name("kademlia_initiator_sent_bytes")
            .help("Total sent bytes in kademlia protocol initiator")
            .register();

    public static final int MAX_MESSAGE_SIZE = 1024*1024;

    private final KademliaEngine engine;

    public KademliaProtocol(KademliaEngine engine) {
        super(Dht.Message.getDefaultInstance(), MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE);
        this.engine = engine;
    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartInitiator(@NotNull Stream stream) {
        engine.addOutgoingConnection(stream.remotePeerId());
        ReplyHandler handler = new ReplyHandler(stream, initiatorSentBytes, initiatorReceivedBytes);
        stream.pushHandler(handler);
        return CompletableFuture.completedFuture(handler);
    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartResponder(@NotNull Stream stream) {
        engine.addIncomingConnection(stream.remotePeerId());
        IncomingRequestHandler handler = new IncomingRequestHandler(engine);
        stream.pushHandler(handler);
        return CompletableFuture.completedFuture(handler);
    }

    class ReplyHandler implements ProtocolMessageHandler<Dht.Message>, KademliaController {
        private final CompletableFuture<Dht.Message> resp = new CompletableFuture<>();
        private final Stream stream;
        private final Counter sentBytes, receivedBytes;

        public ReplyHandler(Stream stream, Counter sentBytes, Counter receivedBytes) {
            this.stream = stream;
            this.sentBytes = sentBytes;
            this.receivedBytes = receivedBytes;
        }

        @Override
        public CompletableFuture<Dht.Message> rpc(Dht.Message msg) {
            stream.writeAndFlush(msg);
            sentBytes.inc(msg.getSerializedSize());
            return resp;
        }

        @Override
        public CompletableFuture<Boolean> send(Dht.Message msg) {
            stream.writeAndFlush(msg);
            sentBytes.inc(msg.getSerializedSize());
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            receivedBytes.inc(msg.getSerializedSize());
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
