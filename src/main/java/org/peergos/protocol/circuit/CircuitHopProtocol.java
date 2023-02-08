package org.peergos.protocol.circuit;

import com.google.protobuf.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.libp2p.core.Stream;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;
import org.peergos.*;
import org.peergos.protocol.circuit.pb.*;
import org.peergos.protocol.crypto.pb.*;

import java.io.*;
import java.nio.charset.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.*;

public class CircuitHopProtocol extends ProtobufProtocolHandler<CircuitHopProtocol.HopController> {

    public static class Binding extends StrictProtocolBinding<HopController> {
        public Binding(RelayManager manager, Supplier<List<MultiAddress>> publicAddresses) {
            super("/libp2p/circuit/relay/0.2.0/hop", new CircuitHopProtocol(manager, publicAddresses));
        }
    }

    public static byte[] createVoucher(PrivKey priv,
                                       Multihash relayPeerId,
                                       Multihash requestorPeerId,
                                       LocalDateTime expiry) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            Multihash.putUvarint(bout, 0x0302);
        } catch (IOException e) {}
        byte[] typeMulticodec = bout.toByteArray();
        byte[] payload = VoucherOuterClass.Voucher.newBuilder()
                .setRelay(ByteString.copyFrom(relayPeerId.toBytes()))
                .setPeer(ByteString.copyFrom(requestorPeerId.toBytes()))
                .setExpiration(expiry.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000)
                .build().toByteArray();
        byte[] signDomain = "libp2p-relay-rsvp".getBytes(StandardCharsets.UTF_8);
        byte[] toSign = new byte[signDomain.length + payload.length];
        System.arraycopy(signDomain, 0, toSign, 0, toSign.length);
        System.arraycopy(payload, 0, toSign, toSign.length, payload.length);
        byte[] signature = priv.sign(toSign);
        return Crypto.Envelope.newBuilder()
                .setPayloadType(ByteString.copyFrom(typeMulticodec))
                .setPayload(ByteString.copyFrom(payload))
                .setPublicKey(Crypto.PublicKey.newBuilder()
                        .setTypeValue(priv.publicKey().getKeyType().getNumber())
                        .setData(ByteString.copyFrom(priv.publicKey().raw())))
                .setSignature(ByteString.copyFrom(signature))
                .build().toByteArray();
    }

    static class Reservation {
        public final LocalDateTime expiry;
        public final int maxSeconds;
        public final long maxBytes;
        public final byte[] voucher;

        public Reservation(LocalDateTime expiry, int maxSeconds, long maxBytes, byte[] voucher) {
            this.expiry = expiry;
            this.maxSeconds = maxSeconds;
            this.maxBytes = maxBytes;
            this.voucher = voucher;
        }
    }

    public interface RelayManager {
        Optional<Reservation> createReservation(Multihash requestor, Stream stream);

        boolean allowConnection(Multihash target, Multihash initiator);

        boolean hasReservation(Multihash source);
    }

    public interface HopController {
        CompletableFuture<Circuit.HopMessage> rpc(Circuit.HopMessage req);

        default CompletableFuture<Circuit.HopMessage> reserve(PeerAddresses us) {
            return rpc(Circuit.HopMessage.newBuilder()
                    .setType(Circuit.HopMessage.Type.RESERVE)
                    .build());
        }

        default CompletableFuture<Circuit.HopMessage> connect(Multihash targetPeerId) {
            return rpc(Circuit.HopMessage.newBuilder()
                    .setType(Circuit.HopMessage.Type.CONNECT)
                    .setPeer(Circuit.Peer.newBuilder().setId(ByteString.copyFrom(targetPeerId.toBytes())))
                    .build());
        }
    }

    public static class Sender implements ProtocolMessageHandler<Circuit.HopMessage>, HopController {
        private final Stream stream;
        private final LinkedBlockingDeque<CompletableFuture<Circuit.HopMessage>> queue = new LinkedBlockingDeque<>();

        public Sender(Stream stream) {
            this.stream = stream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Circuit.HopMessage msg) {
            queue.poll().complete(msg);
        }

        public CompletableFuture<Circuit.HopMessage> rpc(Circuit.HopMessage req) {
            CompletableFuture<Circuit.HopMessage> res = new CompletableFuture<>();
            queue.add(res);
            stream.writeAndFlush(req);
            return res;
        }
    }

    public static class Receiver implements ProtocolMessageHandler<Circuit.HopMessage>, HopController {
        private final Stream p2pstream;
        private final RelayManager manager;
        private final Supplier<List<MultiAddress>> publicAddresses;

        public Receiver(Stream p2pstream, RelayManager manager, Supplier<List<MultiAddress>> publicAddresses) {
            this.p2pstream = p2pstream;
            this.manager = manager;
            this.publicAddresses = publicAddresses;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Circuit.HopMessage msg) {
            switch (msg.getType()) {
                case RESERVE: {
                    Multihash requestor = Multihash.deserialize(stream.remotePeerId().getBytes());
                    Optional<Reservation> reservation = manager.createReservation(requestor, stream);
                    if (reservation.isEmpty()) {
                        stream.writeAndFlush(Circuit.HopMessage.newBuilder()
                                .setType(Circuit.HopMessage.Type.STATUS)
                                .setStatus(Circuit.Status.RESERVATION_REFUSED));
                        return;
                    }
                    Reservation resv = reservation.get();
                    stream.writeAndFlush(Circuit.HopMessage.newBuilder()
                            .setType(Circuit.HopMessage.Type.STATUS)
                            .setStatus(Circuit.Status.OK)
                            .setReservation(Circuit.Reservation.newBuilder()
                                    .setExpire(resv.expiry.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L)
                                    .addAllAddrs(publicAddresses.get().stream()
                                            .map(a -> ByteString.copyFrom(a.getBytes()))
                                            .collect(Collectors.toList()))
                                    .setVoucher(ByteString.copyFrom(resv.voucher)))
                            .setLimit(Circuit.Limit.newBuilder()
                                    .setDuration(resv.maxSeconds)
                                    .setData(resv.maxBytes)));
                } case CONNECT: {
                    Multihash targetPeerId = Multihash.deserialize(msg.getPeer().getId().toByteArray());
                    if (manager.hasReservation(targetPeerId)) {
                        Multihash initiator = Multihash.deserialize(stream.remotePeerId().getBytes());
                        if (manager.allowConnection(targetPeerId, initiator)) {
                            // TODO

                        } else {
                            stream.writeAndFlush(Circuit.HopMessage.newBuilder()
                                    .setType(Circuit.HopMessage.Type.STATUS)
                                    .setStatus(Circuit.Status.RESOURCE_LIMIT_EXCEEDED));
                        }
                    } else {
                        stream.writeAndFlush(Circuit.HopMessage.newBuilder()
                                .setType(Circuit.HopMessage.Type.STATUS)
                                .setStatus(Circuit.Status.NO_RESERVATION));
                    }
                }
            }
        }

        public CompletableFuture<Circuit.HopMessage> rpc(Circuit.HopMessage msg) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send form a receiver!"));
        }
    }

    private static final int TRAFFIC_LIMIT = 2*1024;
    private final RelayManager manager;
    private final Supplier<List<MultiAddress>> publicAddresses;

    public CircuitHopProtocol(RelayManager manager, Supplier<List<MultiAddress>> publicAddresses) {
        super(Circuit.HopMessage.getDefaultInstance(), TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.manager = manager;
        this.publicAddresses = publicAddresses;
    }

    @NotNull
    @Override
    protected CompletableFuture<HopController> onStartInitiator(@NotNull Stream stream) {
        Sender replyPropagator = new Sender(stream);
        stream.pushHandler(replyPropagator);
        return CompletableFuture.completedFuture(replyPropagator);
    }

    @NotNull
    @Override
    protected CompletableFuture<HopController> onStartResponder(@NotNull Stream stream) {
        Receiver dialer = new Receiver(stream, manager, publicAddresses);
        stream.pushHandler(dialer);
        return CompletableFuture.completedFuture(dialer);
    }
}
