package org.peergos.protocol.circuit;

import com.google.protobuf.*;
import io.ipfs.multihash.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;
import org.peergos.protocol.circuit.pb.*;

import java.util.concurrent.*;

public class CircuitStopProtocol extends ProtobufProtocolHandler<CircuitStopProtocol.StopController> {

    public static class Binding extends StrictProtocolBinding<CircuitStopProtocol.StopController> {
        public Binding(StreamUpgrader upgrader) {
            super("/libp2p/circuit/relay/0.2.0/stop", new CircuitStopProtocol(upgrader));
        }
    }

    public interface StopController {
        CompletableFuture<Circuit.StopMessage> rpc(Circuit.StopMessage req);

        Stream getStream();

        default CompletableFuture<Circuit.StopMessage> connect(Multihash sourcePeerId,
                                                               int durationSeconds,
                                                               long maxBytes) {
            return rpc(Circuit.StopMessage.newBuilder()
                    .setType(Circuit.StopMessage.Type.CONNECT)
                    .setPeer(Circuit.Peer.newBuilder().setId(ByteString.copyFrom(sourcePeerId.toBytes())))
                    .setLimit(Circuit.Limit.newBuilder()
                            .setData(maxBytes)
                            .setDuration(durationSeconds))
                    .build());
        }
    }

    public static class Sender implements ProtocolMessageHandler<Circuit.StopMessage>, StopController {
        private final Stream stream;
        private final LinkedBlockingDeque<CompletableFuture<Circuit.StopMessage>> queue = new LinkedBlockingDeque<>();

        public Sender(Stream stream) {
            this.stream = stream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Circuit.StopMessage msg) {
            queue.poll().complete(msg);
        }

        public CompletableFuture<Circuit.StopMessage> rpc(Circuit.StopMessage req) {
            CompletableFuture<Circuit.StopMessage> res = new CompletableFuture<>();
            queue.add(res);
            stream.writeAndFlush(req);
            return res;
        }

        public Stream getStream() {
            return stream;
        }
    }

    public static class Receiver implements ProtocolMessageHandler<Circuit.StopMessage>, StopController {
        private final Stream stream;
        private final StreamUpgrader upgrader;

        public Receiver(Stream stream, StreamUpgrader upgrader) {
            this.stream = stream;
            this.upgrader = upgrader;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Circuit.StopMessage msg) {
            if (msg.getType() == Circuit.StopMessage.Type.CONNECT) {
                Multihash targetPeerId = Multihash.deserialize(msg.getPeer().getId().toByteArray());
                int durationSeconds = msg.getLimit().getDuration();
                long limitBytes = msg.getLimit().getData();
                stream.writeAndFlush(Circuit.StopMessage.newBuilder()
                        .setType(Circuit.StopMessage.Type.STATUS).setStatus(Circuit.Status.OK)
                        .build());
                // now upgrade connection with security and muxer protocol
                System.out.println("Upgrading relayed incoming connection..");
                upgrader.upgrade(stream, targetPeerId, durationSeconds, limitBytes);
            }
        }

        public Stream getStream() {
            return stream;
        }

        public CompletableFuture<Circuit.StopMessage> rpc(Circuit.StopMessage msg) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send form a receiver!"));
        }
    }

    private static final int TRAFFIC_LIMIT = 2*1024;

    private final StreamUpgrader upgrader;

    public CircuitStopProtocol(StreamUpgrader upgrader) {
        super(Circuit.StopMessage.getDefaultInstance(), TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.upgrader = upgrader;
    }

    @NotNull
    @Override
    protected CompletableFuture<StopController> onStartInitiator(@NotNull Stream stream) {
        Sender replyPropagator = new Sender(stream);
        stream.pushHandler(replyPropagator);
        return CompletableFuture.completedFuture(replyPropagator);
    }

    @NotNull
    @Override
    protected CompletableFuture<StopController> onStartResponder(@NotNull Stream stream) {
        Receiver acceptor = new Receiver(stream, upgrader);
        stream.pushHandler(acceptor);
        return CompletableFuture.completedFuture(acceptor);
    }
}
