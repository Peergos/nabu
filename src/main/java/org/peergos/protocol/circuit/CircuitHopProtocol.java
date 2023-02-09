package org.peergos.protocol.circuit;

import com.google.protobuf.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
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

    public static class Binding extends StrictProtocolBinding<HopController> implements HostConsumer {
        private final CircuitHopProtocol hop;

        private Binding(CircuitHopProtocol hop) {
            super("/libp2p/circuit/relay/0.2.0/hop", hop);
            this.hop = hop;
        }

        public Binding(RelayManager manager, CircuitStopProtocol.Binding stop) {
            this(new CircuitHopProtocol(manager, stop));
        }

        @Override
        public void setHost(Host us) {
            hop.setHost(us);
        }

        public CircuitHopProtocol getHop() {
            return hop;
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
        ByteArrayOutputStream toSign = new ByteArrayOutputStream();
        try {
            Multihash.putUvarint(toSign, signDomain.length);
            toSign.write(signDomain);
            Multihash.putUvarint(toSign, typeMulticodec.length);
            toSign.write(typeMulticodec);
            Multihash.putUvarint(toSign, payload.length);
            toSign.write(payload);
        } catch (IOException e) {}
        byte[] signature = priv.sign(toSign.toByteArray());
        return Crypto.Envelope.newBuilder()
                .setPayloadType(ByteString.copyFrom(typeMulticodec))
                .setPayload(ByteString.copyFrom(payload))
                .setPublicKey(Crypto.PublicKey.newBuilder()
                        .setTypeValue(priv.publicKey().getKeyType().getNumber())
                        .setData(ByteString.copyFrom(priv.publicKey().raw())))
                .setSignature(ByteString.copyFrom(signature))
                .build().toByteArray();
    }

    public static class Reservation {
        public final LocalDateTime expiry;
        public final int durationSeconds;
        public final long maxBytes;
        public final byte[] voucher;

        public Reservation(LocalDateTime expiry, int durationSeconds, long maxBytes, byte[] voucher) {
            this.expiry = expiry;
            this.durationSeconds = durationSeconds;
            this.maxBytes = maxBytes;
            this.voucher = voucher;
        }
    }

    public interface RelayManager {
        boolean hasReservation(Multihash source);

        Optional<Reservation> createReservation(Multihash requestor);

        Optional<Reservation> allowConnection(Multihash target, Multihash initiator);

        static RelayManager limitTo(PrivKey priv, Multihash relayPeerId, int concurrent) {
            return new RelayManager() {
                Map<Multihash, Reservation> reservations = new HashMap<>();

                @Override
                public synchronized boolean hasReservation(Multihash source) {
                    return reservations.containsKey(source);
                }

                @Override
                public synchronized Optional<Reservation> createReservation(Multihash requestor) {
                    if (reservations.size() >= concurrent)
                        return Optional.empty();
                    LocalDateTime now = LocalDateTime.now();
                    LocalDateTime expiry = now.plusHours(1);
                    byte[] voucher = createVoucher(priv, relayPeerId, requestor, now);
                    Reservation resv = new Reservation(expiry, 3600, 4096, voucher);
                    reservations.put(requestor, resv);
                    return Optional.of(resv);
                }

                @Override
                public synchronized Optional<Reservation> allowConnection(Multihash target, Multihash initiator) {
                    return Optional.ofNullable(reservations.get(target));
                }
            };
        }
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
        private final Host us;
        private final RelayManager manager;
        private final Supplier<List<MultiAddress>> publicAddresses;
        private final CircuitStopProtocol.Binding stop;
        private final AddressBook addressBook;

        public Receiver(Host us,
                        RelayManager manager,
                        Supplier<List<MultiAddress>> publicAddresses,
                        CircuitStopProtocol.Binding stop,
                        AddressBook addressBook) {
            this.us = us;
            this.manager = manager;
            this.publicAddresses = publicAddresses;
            this.stop = stop;
            this.addressBook = addressBook;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Circuit.HopMessage msg) {
            switch (msg.getType()) {
                case RESERVE: {
                    Multihash requestor = Multihash.deserialize(stream.remotePeerId().getBytes());
                    Optional<Reservation> reservation = manager.createReservation(requestor);
                    if (reservation.isEmpty() || new MultiAddress(stream.getConnection().remoteAddress().toString()).isRelayed()) {
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
                                    .setDuration(resv.durationSeconds)
                                    .setData(resv.maxBytes)));
                } case CONNECT: {
                    Multihash targetPeerId = Multihash.deserialize(msg.getPeer().getId().toByteArray());
                    if (manager.hasReservation(targetPeerId)) {
                        Multihash initiator = Multihash.deserialize(stream.remotePeerId().getBytes());
                        Optional<Reservation> res = manager.allowConnection(targetPeerId, initiator);
                        if (res.isPresent()) {
                            Reservation resv = res.get();
                            PeerId target = PeerId.fromBase58(targetPeerId.toBase58());
                            CircuitStopProtocol.StopController stop = this.stop.dial(us, target,
                                    addressBook.getAddrs(target).join().toArray(new Multiaddr[0])).getController().join();
                            Circuit.StopMessage reply = stop.connect(initiator, resv.durationSeconds, resv.maxBytes).join();
                            if (reply.getStatus().equals(Circuit.Status.OK)) {
                                stream.writeAndFlush(Circuit.StopMessage.newBuilder()
                                    .setType(Circuit.StopMessage.Type.STATUS)
                                    .setStatus(Circuit.Status.OK));
                                Stream toTarget = stop.getStream();
                                Stream fromRequestor = stream;
                                // TODO connect these streams with time + bytes enforcement
//                                toTarget.pushHandler();
//                                fromRequestor.pushHandler();
                            } else {
                                stream.writeAndFlush(Circuit.HopMessage.newBuilder()
                                    .setType(Circuit.HopMessage.Type.STATUS)
                                    .setStatus(reply.getStatus()));
                            }
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
    private final CircuitStopProtocol.Binding stop;
    private Host us;

    public CircuitHopProtocol(RelayManager manager,
                              CircuitStopProtocol.Binding stop) {
        super(Circuit.HopMessage.getDefaultInstance(), TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.manager = manager;
        this.stop = stop;
    }

    public void setHost(Host us) {
        this.us = us;
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
        if (us == null)
            throw new IllegalStateException("null Host for us!");
        Supplier<List<MultiAddress>> ourpublicAddresses = () -> us.getAddressBook().get(us.getPeerId()).join()
                .stream()
                .map(a -> new MultiAddress(a.toString()))
                .collect(Collectors.toList());
        Receiver dialer = new Receiver(us, manager, ourpublicAddresses, stop, us.getAddressBook());
        stream.pushHandler(dialer);
        return CompletableFuture.completedFuture(dialer);
    }
}
