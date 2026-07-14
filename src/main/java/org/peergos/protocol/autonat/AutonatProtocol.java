package org.peergos.protocol.autonat;

import com.google.protobuf.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.core.transport.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;
import org.peergos.*;
import org.peergos.protocol.autonat.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

public class AutonatProtocol extends ProtobufProtocolHandler<AutonatProtocol.AutoNatController> {

    public static final String PROTOCOL = "/libp2p/autonat/v1.0.0";
    /** Cap on dial-back attempts per DIAL request, to bound resource use. */
    private static final int MAX_DIALS = 3;
    /** How long to wait for a dial-back to succeed before treating it as unreachable. */
    private static final int DIAL_TIMEOUT_SECONDS = 15;

    public static class Binding extends StrictProtocolBinding<AutoNatController> implements HostConsumer {
        private final AutonatProtocol autonat;

        private Binding(AutonatProtocol autonat) {
            super(PROTOCOL, autonat);
            this.autonat = autonat;
        }

        public Binding() {
            this(new AutonatProtocol());
        }

        @Override
        public void setHost(Host us) {
            autonat.setHost(us);
        }
    }

    public interface AutoNatController {
        CompletableFuture<Autonat.Message> rpc(Autonat.Message req);

        default CompletableFuture<Autonat.Message.DialResponse> requestDial(PeerAddresses us) {
            return rpc(Autonat.Message.newBuilder()
                    .setType(Autonat.Message.MessageType.DIAL)
                    .setDial(Autonat.Message.Dial.newBuilder()
                            .setPeer(Autonat.Message.PeerInfo.newBuilder()
                                    .addAllAddrs(us.addresses.stream()
                                            .map(a -> ByteString.copyFrom(a.serialize()))
                                            .collect(Collectors.toList()))
                                    .setId(ByteString.copyFrom(us.peerId.toBytes()))))
                    .build())
                    .thenApply(msg -> msg.getDialResponse());
        }
    }

    public static class Sender implements ProtocolMessageHandler<Autonat.Message>, AutoNatController {
        private final Stream stream;
        private final LinkedBlockingDeque<CompletableFuture<Autonat.Message>> queue = new LinkedBlockingDeque<>();

        public Sender(Stream stream) {
            this.stream = stream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Autonat.Message msg) {
            queue.poll().complete(msg);
        }

        public CompletableFuture<Autonat.Message> rpc(Autonat.Message req) {
            CompletableFuture<Autonat.Message> res = new CompletableFuture<>();
            queue.add(res);
            stream.writeAndFlush(req);
            return res;
        }
    }

    /**
     * Select which of a peer's claimed addresses we are willing to dial back. Anti-amplification: only
     * dial addresses whose host matches the address we observed the requester connecting from, so a peer
     * can never make us dial an arbitrary third party. Only public addresses are considered.
     */
    public static List<Multiaddr> selectDialBackAddresses(List<Multiaddr> claimed, String observedIp, int max) {
        if (observedIp == null)
            return List.of();
        return claimed.stream()
                .filter(a -> PeerAddresses.isPublic(a, false))
                .filter(a -> observedIp.equals(new MultiAddress(a.toString()).getHost()))
                .distinct()
                .limit(max)
                .collect(Collectors.toList());
    }

    /**
     * Force a fresh connection to {@code target} at {@code addr} and confirm the peer identity matches.
     * A fresh connection is required (rather than {@link Network#connect}, which reuses any existing
     * connection to the peer) so that we actually test whether the address is dialable.
     */
    public static CompletableFuture<Boolean> dialBack(Host us, PeerId target, Multiaddr addr) {
        // Ensure the dial address carries the expected peer id, without clobbering a conflicting one.
        Multiaddr full = addr.has(io.libp2p.core.multiformats.Protocol.P2P) ? addr : addr.withP2P(target);
        Optional<Transport> transport = us.getNetwork().getTransports().stream()
                .filter(t -> t.handles(full))
                .findFirst();
        if (transport.isEmpty())
            return CompletableFuture.completedFuture(false);
        CompletableFuture<Boolean> res = new CompletableFuture<>();
        CompletableFuture<Connection> dial;
        try {
            dial = transport.get().dial(full, conn -> {});
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
        dial.whenComplete((conn, err) -> {
            if (err != null || conn == null) {
                res.complete(false);
                return;
            }
            boolean ok = Arrays.equals(conn.secureSession().getRemoteId().getBytes(), target.getBytes());
            conn.close();
            res.complete(ok);
        });
        return res.completeOnTimeout(false, DIAL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /** Dial back all candidate addresses in parallel, completing with the first that succeeds. */
    public static CompletableFuture<Optional<Multiaddr>> dialBackAny(Host us, PeerId target, List<Multiaddr> addrs) {
        if (addrs.isEmpty())
            return CompletableFuture.completedFuture(Optional.empty());
        CompletableFuture<Optional<Multiaddr>> result = new CompletableFuture<>();
        AtomicInteger remaining = new AtomicInteger(addrs.size());
        for (Multiaddr addr : addrs) {
            dialBack(us, target, addr).whenComplete((success, err) -> {
                if (Boolean.TRUE.equals(success))
                    result.complete(Optional.of(addr));
                else if (remaining.decrementAndGet() == 0)
                    result.complete(Optional.empty());
            });
        }
        return result;
    }

    public static class Receiver implements ProtocolMessageHandler<Autonat.Message>, AutoNatController {
        private final Host us;
        private final Stream p2pstream;

        public Receiver(Host us, Stream p2pstream) {
            this.us = us;
            this.p2pstream = p2pstream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Autonat.Message msg) {
            switch (msg.getType()) {
                case DIAL: {
                    Autonat.Message.Dial dial = msg.getDial();
                    Multihash peerId = Multihash.deserialize(dial.getPeer().getId().toByteArray());
                    PeerId streamPeerId = stream.remotePeerId();
                    if (! Arrays.equals(peerId.toBytes(), streamPeerId.getBytes())) {
                        respondError(Autonat.Message.ResponseStatus.E_BAD_REQUEST);
                        return;
                    }

                    Multiaddr remote = stream.getConnection().remoteAddress();
                    String remoteIp = new MultiAddress(remote.toString()).getHost();
                    List<Multiaddr> claimed = dial.getPeer().getAddrsList().stream()
                            .flatMap(b -> {
                                try {
                                    return java.util.stream.Stream.of(Multiaddr.deserialize(b.toByteArray()));
                                } catch (Exception e) {
                                    return java.util.stream.Stream.empty();
                                }
                            })
                            .collect(Collectors.toList());
                    List<Multiaddr> toDial = selectDialBackAddresses(claimed, remoteIp, MAX_DIALS);
                    if (toDial.isEmpty()) {
                        respondError(Autonat.Message.ResponseStatus.E_DIAL_ERROR);
                        return;
                    }
                    PeerId target = PeerId.fromBase58(peerId.toBase58());
                    dialBackAny(us, target, toDial).thenAccept(reachable -> {
                        Autonat.Message.Builder resp = Autonat.Message.newBuilder()
                                .setType(Autonat.Message.MessageType.DIAL_RESPONSE);
                        if (reachable.isPresent()) {
                            resp.setDialResponse(Autonat.Message.DialResponse.newBuilder()
                                    .setStatus(Autonat.Message.ResponseStatus.OK)
                                    .setAddr(ByteString.copyFrom(reachable.get().serialize())));
                        } else {
                            resp.setDialResponse(Autonat.Message.DialResponse.newBuilder()
                                    .setStatus(Autonat.Message.ResponseStatus.E_DIAL_ERROR));
                        }
                        p2pstream.writeAndFlush(resp.build());
                    });
                }
                default: {}
            }
        }

        private void respondError(Autonat.Message.ResponseStatus status) {
            p2pstream.writeAndFlush(Autonat.Message.newBuilder()
                    .setType(Autonat.Message.MessageType.DIAL_RESPONSE)
                    .setDialResponse(Autonat.Message.DialResponse.newBuilder().setStatus(status))
                    .build());
        }

        public CompletableFuture<Autonat.Message> rpc(Autonat.Message msg) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send form a receiver!"));
        }
    }

    private static final int TRAFFIC_LIMIT = 2*1024;
    private Host us;

    public AutonatProtocol() {
        super(Autonat.Message.getDefaultInstance(), TRAFFIC_LIMIT, TRAFFIC_LIMIT);
    }

    public void setHost(Host us) {
        this.us = us;
    }

    @NotNull
    @Override
    protected CompletableFuture<AutoNatController> onStartInitiator(@NotNull Stream stream) {
        Sender replyPropagator = new Sender(stream);
        stream.pushHandler(replyPropagator);
        return CompletableFuture.completedFuture(replyPropagator);
    }

    @NotNull
    @Override
    protected CompletableFuture<AutoNatController> onStartResponder(@NotNull Stream stream) {
        if (us == null)
            throw new IllegalStateException("null Host for us!");
        Receiver dialer = new Receiver(us, stream);
        stream.pushHandler(dialer);
        return CompletableFuture.completedFuture(dialer);
    }
}
