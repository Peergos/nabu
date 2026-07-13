package org.peergos.protocol.dcutr;

import com.google.protobuf.*;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.core.transport.*;
import io.libp2p.protocol.*;
import io.libp2p.transport.quic.*;
import org.jetbrains.annotations.*;
import org.peergos.HostConsumer;
import org.peergos.protocol.dcutr.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.Collectors;

/**
 * DCUtR - Direct Connection Upgrade through Relay (https://github.com/libp2p/specs/blob/master/relay/DCUtR.md).
 *
 * Once a relayed connection exists, the two peers coordinate a simultaneous connect over it to establish
 * a direct connection. The inbound peer (B, the one dialed via the relay) opens the {@code /libp2p/dcutr}
 * stream, sends CONNECT with its dialable addresses, measures the round trip to the outbound peer's
 * CONNECT reply, then sends SYNC and — half an RTT later — hole-punches to A. The outbound peer (A) replies
 * with its addresses and, on receiving SYNC, immediately dials B (A is the client of the simultaneous open).
 */
public class DcutrProtocol extends ProtobufProtocolHandler<DcutrProtocol.Controller> {

    public static final String PROTOCOL = "/libp2p/dcutr";
    private static final Logger LOG = Logging();
    private static final int TRAFFIC_LIMIT = 4 * 1024;
    private static final int MAX_DIAL_ATTEMPTS = 4;

    private static Logger Logging() {
        try {
            return org.peergos.util.Logging.LOG();
        } catch (Throwable t) {
            return Logger.getLogger(DcutrProtocol.class.getName());
        }
    }

    public interface Controller {
        /** Completes with the direct connection once the hole punch succeeds. */
        CompletableFuture<Connection> directConnection();
    }

    public static class Binding extends StrictProtocolBinding<Controller> implements HostConsumer {
        private final DcutrProtocol dcutr;

        public Binding(Supplier<List<Multiaddr>> extraDialableAddresses, Consumer<Connection> onUpgraded) {
            this(new DcutrProtocol(extraDialableAddresses, onUpgraded));
        }

        private Binding(DcutrProtocol dcutr) {
            super(PROTOCOL, dcutr);
            this.dcutr = dcutr;
        }

        @Override
        public void setHost(Host us) {
            dcutr.setHost(us);
        }
    }

    private Host us;
    private final Supplier<List<Multiaddr>> extraDialableAddresses;
    private final Consumer<Connection> onUpgraded;
    private final ScheduledExecutorService timer;

    public DcutrProtocol(Supplier<List<Multiaddr>> extraDialableAddresses, Consumer<Connection> onUpgraded) {
        super(Dcutr.HolePunch.getDefaultInstance(), TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.extraDialableAddresses = extraDialableAddresses;
        this.onUpgraded = onUpgraded;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dcutr");
            t.setDaemon(true);
            return t;
        });
    }

    public void setHost(Host us) {
        this.us = us;
    }

    /** Our directly dialable addresses to advertise to the peer (never the relayed ones). */
    private List<Multiaddr> dialableAddresses() {
        List<Multiaddr> addrs = new ArrayList<>();
        if (us != null)
            addrs.addAll(us.listenAddresses());
        if (extraDialableAddresses != null)
            addrs.addAll(extraDialableAddresses.get());
        return addrs.stream()
                .filter(a -> ! a.toString().contains("p2p-circuit"))
                .distinct()
                .collect(Collectors.toList());
    }

    @NotNull
    @Override
    protected CompletableFuture<Controller> onStartInitiator(@NotNull Stream stream) {
        Session session = new Session(us, stream, true, this::dialableAddresses, timer, onUpgraded);
        stream.pushHandler(session);
        return CompletableFuture.completedFuture(session);
    }

    @NotNull
    @Override
    protected CompletableFuture<Controller> onStartResponder(@NotNull Stream stream) {
        Session session = new Session(us, stream, false, this::dialableAddresses, timer, onUpgraded);
        stream.pushHandler(session);
        return CompletableFuture.completedFuture(session);
    }

    static class Session implements ProtocolMessageHandler<Dcutr.HolePunch>, Controller {
        private final Host us;
        private final Stream stream;
        private final boolean initiator; // true == B (opened the stream, hole-punches after RTT/2)
        private final Supplier<List<Multiaddr>> ourAddresses;
        private final ScheduledExecutorService timer;
        private final Consumer<Connection> onUpgraded;
        private final CompletableFuture<Connection> direct = new CompletableFuture<>();
        private volatile long connectSentAtNanos;
        private volatile List<Multiaddr> peerAddresses = List.of();

        Session(Host us, Stream stream, boolean initiator, Supplier<List<Multiaddr>> ourAddresses,
                ScheduledExecutorService timer, Consumer<Connection> onUpgraded) {
            this.us = us;
            this.stream = stream;
            this.initiator = initiator;
            this.ourAddresses = ourAddresses;
            this.timer = timer;
            this.onUpgraded = onUpgraded;
        }

        @Override
        public CompletableFuture<Connection> directConnection() {
            return direct;
        }

        @Override
        public void onActivated(@NotNull Stream stream) {
            // B (initiator) starts the exchange once the stream is ready to write
            if (initiator) {
                connectSentAtNanos = System.nanoTime();
                send(Dcutr.HolePunch.Type.CONNECT);
            }
        }

        private void send(Dcutr.HolePunch.Type type) {
            stream.writeAndFlush(Dcutr.HolePunch.newBuilder()
                    .setType(type)
                    .addAllObsAddrs(ourAddresses.get().stream()
                            .map(a -> ByteString.copyFrom(a.serialize()))
                            .collect(Collectors.toList()))
                    .build());
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dcutr.HolePunch msg) {
            List<Multiaddr> peer = msg.getObsAddrsList().stream()
                    .flatMap(b -> {
                        try {
                            return java.util.stream.Stream.of(Multiaddr.deserialize(b.toByteArray()));
                        } catch (Exception e) {
                            return java.util.stream.Stream.empty();
                        }
                    })
                    .collect(Collectors.toList());
            switch (msg.getType()) {
                case CONNECT: {
                    peerAddresses = peer;
                    if (initiator) {
                        // B received A's CONNECT reply: measure RTT, SYNC, then punch after half the RTT
                        long rttNanos = System.nanoTime() - connectSentAtNanos;
                        send(Dcutr.HolePunch.Type.SYNC);
                        long halfRttMillis = Math.max(0, rttNanos / 2 / 1_000_000);
                        List<Multiaddr> targets = peer;
                        timer.schedule(() -> holePunch(targets), halfRttMillis, TimeUnit.MILLISECONDS);
                    } else {
                        // A received B's CONNECT: reply with our addresses (A dials on SYNC)
                        send(Dcutr.HolePunch.Type.CONNECT);
                    }
                    break;
                }
                case SYNC: {
                    if (! initiator) // A dials immediately on SYNC
                        holePunch(peerAddresses);
                    break;
                }
                default: {}
            }
        }

        private void holePunch(List<Multiaddr> targets) {
            PeerId target = stream.remotePeerId();
            targets.stream().limit(MAX_DIAL_ATTEMPTS).forEach(addr ->
                    dialDirect(target, addr).thenAccept(conn -> {
                        if (conn != null && direct.complete(conn)) {
                            LOG.info("DCUtR established a direct connection to " + target + " at " + conn.remoteAddress());
                            if (onUpgraded != null)
                                onUpgraded.accept(conn);
                        }
                    }));
        }

        private CompletableFuture<Connection> dialDirect(PeerId target, Multiaddr addr) {
            if (us == null)
                return CompletableFuture.completedFuture(null);
            Multiaddr full = addr.has(Protocol.P2P) ? addr : addr.withP2P(target);
            Optional<Transport> transport = us.getNetwork().getTransports().stream()
                    .filter(t -> t.handles(full))
                    .findFirst();
            if (transport.isEmpty())
                return CompletableFuture.completedFuture(null);
            try {
                boolean quic = full.has(Protocol.QUICV1);
                CompletableFuture<Connection> dialed =
                        (initiator && quic && transport.get() instanceof QuicTransport)
                                ? ((QuicTransport) transport.get()).dialAsListener(full, c -> {}, null)
                                : transport.get().dial(full, c -> {});
                return dialed.handle((conn, err) -> err == null ? conn : null);
            } catch (Exception e) {
                return CompletableFuture.completedFuture(null);
            }
        }
    }
}
