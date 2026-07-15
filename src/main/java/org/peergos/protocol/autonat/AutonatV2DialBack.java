package org.peergos.protocol.autonat;

import io.libp2p.core.Stream;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import io.netty.handler.codec.protobuf.*;
import org.jetbrains.annotations.*;
import org.peergos.protocol.autonat.pb.*;

import java.util.concurrent.*;

/**
 * The AutoNAT v2 dial-back protocol ({@code /libp2p/autonat/2/dial-back}). The server opens this stream
 * on the connection it just dialed and sends a {@link Autonatv2.DialBack} carrying the client's nonce;
 * the client verifies the nonce and replies {@link Autonatv2.DialBackResponse}. The two directions carry
 * different message types, so each side installs its own decoder (the base protobuf handler can't).
 */
public class AutonatV2DialBack extends ProtocolHandler<AutonatV2DialBack.DialBackController> {

    public static final String PROTOCOL = "/libp2p/autonat/2/dial-back";
    private static final int TRAFFIC_LIMIT = 4 * 1024;

    private final NonceRegistry registry;

    public AutonatV2DialBack(NonceRegistry registry) {
        super(TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.registry = registry;
    }

    public static class Binding extends StrictProtocolBinding<DialBackController> {
        private final NonceRegistry registry;

        public Binding(NonceRegistry registry) {
            super(PROTOCOL, new AutonatV2DialBack(registry));
            this.registry = registry;
        }

        public NonceRegistry getRegistry() {
            return registry;
        }
    }

    public interface DialBackController {
        /** Server side: send our nonce and complete with true once the client acknowledges it. */
        default CompletableFuture<Boolean> sendNonce(long nonce) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send from the dial-back responder"));
        }
    }

    @Override
    protected void initProtocolStream(@NotNull Stream stream) {
        stream.pushHandler(new ProtobufVarint32FrameDecoder());
        stream.pushHandler(new ProtobufVarint32LengthFieldPrepender());
        stream.pushHandler(new ProtobufEncoder());
    }

    @NotNull
    @Override
    protected CompletableFuture<DialBackController> onStartInitiator(@NotNull Stream stream) {
        stream.pushHandler(new ProtobufDecoder(Autonatv2.DialBackResponse.getDefaultInstance()));
        Sender sender = new Sender(stream);
        stream.pushHandler(sender);
        return CompletableFuture.completedFuture(sender);
    }

    @NotNull
    @Override
    protected CompletableFuture<DialBackController> onStartResponder(@NotNull Stream stream) {
        stream.pushHandler(new ProtobufDecoder(Autonatv2.DialBack.getDefaultInstance()));
        Receiver receiver = new Receiver(stream, registry);
        stream.pushHandler(receiver);
        return CompletableFuture.completedFuture(receiver);
    }

    /** Server side: sends the nonce, awaits the client's acknowledgement. */
    static class Sender implements ProtocolMessageHandler<Autonatv2.DialBackResponse>, DialBackController {
        private final Stream stream;
        private final CompletableFuture<Boolean> acknowledged = new CompletableFuture<>();
        // completes when the stream is writable, so the nonce write never races stream setup
        private final CompletableFuture<Void> activated = new CompletableFuture<>();

        Sender(Stream stream) {
            this.stream = stream;
        }

        @Override
        public CompletableFuture<Boolean> sendNonce(long nonce) {
            activated.thenRun(() -> stream.writeAndFlush(Autonatv2.DialBack.newBuilder().setNonce(nonce).build()));
            return acknowledged;
        }

        @Override
        public void onActivated(@NotNull Stream stream) {
            activated.complete(null);
        }

        @Override
        public void onMessage(@NotNull Stream stream, Autonatv2.DialBackResponse msg) {
            acknowledged.complete(msg.getStatus() == Autonatv2.DialBackResponse.DialBackStatus.OK);
        }
    }

    /** Client side: verifies the nonce came from an address we asked to be tested, then acknowledges. */
    static class Receiver implements ProtocolMessageHandler<Autonatv2.DialBack>, DialBackController {
        private final Stream stream;
        private final NonceRegistry registry;

        Receiver(Stream stream, NonceRegistry registry) {
            this.stream = stream;
            this.registry = registry;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Autonatv2.DialBack msg) {
            if (registry.fulfil(msg.getNonce()))
                stream.writeAndFlush(Autonatv2.DialBackResponse.newBuilder()
                        .setStatus(Autonatv2.DialBackResponse.DialBackStatus.OK)
                        .build());
            else
                stream.close();
        }
    }
}
