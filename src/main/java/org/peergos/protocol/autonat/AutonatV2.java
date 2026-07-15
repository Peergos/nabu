package org.peergos.protocol.autonat;

import com.google.protobuf.*;
import io.ipfs.multiaddr.MultiAddress;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.core.transport.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;
import org.peergos.HostConsumer;
import org.peergos.protocol.autonat.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * AutoNAT v2 dial-request protocol ({@code /libp2p/autonat/2/dial-request}). A client asks a server to
 * dial one of its addresses and prove reachability by echoing a nonce over a separate dial-back stream.
 *
 * As a server we only dial addresses whose IP matches the address we observed the client connecting from
 * (anti-amplification by restriction, as in v1), so we never need the dial-data exchange. As a client we
 * still handle a {@code DialDataRequest} for interop with servers that ask for it.
 */
public class AutonatV2 extends ProtobufProtocolHandler<AutonatV2.AutoNat2Controller> {

    public static final String PROTOCOL = "/libp2p/autonat/2/dial-request";
    private static final int TRAFFIC_LIMIT = 8 * 1024;
    private static final int DIAL_TIMEOUT_SECONDS = 15;
    private static final int DIAL_DATA_CHUNK = 4096;

    public interface AutoNat2Controller {
        default CompletableFuture<Autonatv2.DialResponse> requestDial(List<Multiaddr> addrs, long nonce) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot request from a server"));
        }
    }

    public static class Binding extends StrictProtocolBinding<AutoNat2Controller> implements HostConsumer {
        private final AutonatV2 autonat;

        public Binding(AutonatV2DialBack.Binding dialBack) {
            this(new AutonatV2(dialBack));
        }

        private Binding(AutonatV2 autonat) {
            super(PROTOCOL, autonat);
            this.autonat = autonat;
        }

        public NonceRegistry getNonces() {
            return autonat.dialBack.getRegistry();
        }

        @Override
        public void setHost(Host us) {
            autonat.us = us;
        }
    }

    /** Create the pair of v2 bindings (dial-request + dial-back) that share one nonce registry. */
    public static List<ProtocolBinding> protocols() {
        AutonatV2DialBack.Binding dialBack = new AutonatV2DialBack.Binding(new NonceRegistry());
        return List.of(new Binding(dialBack), dialBack);
    }

    private Host us;
    private final AutonatV2DialBack.Binding dialBack;

    public AutonatV2(AutonatV2DialBack.Binding dialBack) {
        super(Autonatv2.Message.getDefaultInstance(), TRAFFIC_LIMIT, TRAFFIC_LIMIT);
        this.dialBack = dialBack;
    }

    @NotNull
    @Override
    protected CompletableFuture<AutoNat2Controller> onStartInitiator(@NotNull Stream stream) {
        Sender sender = new Sender(stream);
        stream.pushHandler(sender);
        return CompletableFuture.completedFuture(sender);
    }

    @NotNull
    @Override
    protected CompletableFuture<AutoNat2Controller> onStartResponder(@NotNull Stream stream) {
        Receiver receiver = new Receiver(us, stream, dialBack);
        stream.pushHandler(receiver);
        return CompletableFuture.completedFuture(receiver);
    }

    /** Client side: sends the dial request and awaits the response. */
    static class Sender implements ProtocolMessageHandler<Autonatv2.Message>, AutoNat2Controller {
        private final Stream stream;
        private final CompletableFuture<Autonatv2.DialResponse> result = new CompletableFuture<>();
        // completes when the stream is writable, so writes chained off it never race stream setup
        private final CompletableFuture<Void> activated = new CompletableFuture<>();

        Sender(Stream stream) {
            this.stream = stream;
        }

        @Override
        public CompletableFuture<Autonatv2.DialResponse> requestDial(List<Multiaddr> addrs, long nonce) {
            Autonatv2.DialRequest.Builder req = Autonatv2.DialRequest.newBuilder().setNonce(nonce);
            for (Multiaddr a : addrs)
                req.addAddrs(ByteString.copyFrom(a.serialize()));
            Autonatv2.Message msg = Autonatv2.Message.newBuilder().setDialRequest(req).build();
            activated.thenRun(() -> stream.writeAndFlush(msg));
            return result;
        }

        @Override
        public void onActivated(@NotNull Stream stream) {
            activated.complete(null);
        }

        @Override
        public void onMessage(@NotNull Stream stream, Autonatv2.Message msg) {
            if (msg.hasDialResponse()) {
                result.complete(msg.getDialResponse());
            } else if (msg.hasDialDataRequest()) {
                // Some servers require us to send data before dialing addresses on a different IP.
                long remaining = msg.getDialDataRequest().getNumBytes();
                byte[] chunk = new byte[DIAL_DATA_CHUNK];
                while (remaining > 0) {
                    int n = (int) Math.min(remaining, DIAL_DATA_CHUNK);
                    stream.writeAndFlush(Autonatv2.Message.newBuilder()
                            .setDialDataResponse(Autonatv2.DialDataResponse.newBuilder()
                                    .setData(ByteString.copyFrom(chunk, 0, n)))
                            .build());
                    remaining -= n;
                }
            }
        }
    }

    /** Server side: dials one of the requested addresses back and proves it via the dial-back nonce. */
    static class Receiver implements ProtocolMessageHandler<Autonatv2.Message>, AutoNat2Controller {
        private final Host us;
        private final Stream stream;
        private final AutonatV2DialBack.Binding dialBack;

        Receiver(Host us, Stream stream, AutonatV2DialBack.Binding dialBack) {
            this.us = us;
            this.stream = stream;
            this.dialBack = dialBack;
        }

        @Override
        public void onMessage(@NotNull Stream p2pstream, Autonatv2.Message msg) {
            if (! msg.hasDialRequest())
                return;
            Autonatv2.DialRequest req = msg.getDialRequest();
            long nonce = req.getNonce();
            PeerId client = stream.remotePeerId();
            String observedIp = new MultiAddress(stream.getConnection().remoteAddress().toString()).getHost();

            List<Multiaddr> addrs = req.getAddrsList().stream()
                    .flatMap(b -> {
                        try {
                            return java.util.stream.Stream.of(Multiaddr.deserialize(b.toByteArray()));
                        } catch (Exception e) {
                            return java.util.stream.Stream.empty();
                        }
                    })
                    .collect(Collectors.toList());

            // pick the first address on the observed IP that we have a transport for (anti-amplification)
            int chosen = -1;
            Multiaddr chosenAddr = null;
            for (int i = 0; i < addrs.size(); i++) {
                Multiaddr a = addrs.get(i);
                Multiaddr full = a.has(io.libp2p.core.multiformats.Protocol.P2P) ? a : a.withP2P(client);
                boolean sameIp = observedIp != null && observedIp.equals(new MultiAddress(a.toString()).getHost());
                boolean handled = us.getNetwork().getTransports().stream().anyMatch(t -> t.handles(full));
                if (sameIp && handled) {
                    chosen = i;
                    chosenAddr = full;
                    break;
                }
            }
            if (chosen < 0) {
                respond(Autonatv2.DialResponse.ResponseStatus.E_DIAL_REFUSED, 0, Autonatv2.DialStatus.UNUSED);
                return;
            }
            final int addrIdx = chosen;
            dialBackAndProve(chosenAddr, client, nonce)
                    .whenComplete((dialStatus, err) -> respond(Autonatv2.DialResponse.ResponseStatus.OK, addrIdx,
                            err != null ? Autonatv2.DialStatus.E_DIAL_ERROR : dialStatus));
        }

        private CompletableFuture<Autonatv2.DialStatus> dialBackAndProve(Multiaddr full, PeerId client, long nonce) {
            Optional<Transport> transport = us.getNetwork().getTransports().stream()
                    .filter(t -> t.handles(full))
                    .findFirst();
            if (transport.isEmpty())
                return CompletableFuture.completedFuture(Autonatv2.DialStatus.E_DIAL_ERROR);
            CompletableFuture<Autonatv2.DialStatus> res = new CompletableFuture<>();
            CompletableFuture<Connection> dial;
            try {
                dial = transport.get().dial(full, conn -> {});
            } catch (Exception e) {
                return CompletableFuture.completedFuture(Autonatv2.DialStatus.E_DIAL_ERROR);
            }
            dial.orTimeout(DIAL_TIMEOUT_SECONDS, TimeUnit.SECONDS).whenComplete((conn, err) -> {
                if (err != null || conn == null) {
                    res.complete(Autonatv2.DialStatus.E_DIAL_ERROR);
                    return;
                }
                if (! Arrays.equals(conn.secureSession().getRemoteId().getBytes(), client.getBytes())) {
                    conn.close();
                    res.complete(Autonatv2.DialStatus.E_DIAL_ERROR);
                    return;
                }
                conn.muxerSession().createStream(dialBack).getController()
                        .thenCompose(c -> c.sendNonce(nonce))
                        .orTimeout(DIAL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .whenComplete((acked, e2) -> {
                            conn.close();
                            res.complete(Boolean.TRUE.equals(acked)
                                    ? Autonatv2.DialStatus.OK
                                    : Autonatv2.DialStatus.E_DIAL_BACK_ERROR);
                        });
            });
            return res;
        }

        private void respond(Autonatv2.DialResponse.ResponseStatus status, int addrIdx, Autonatv2.DialStatus dialStatus) {
            stream.writeAndFlush(Autonatv2.Message.newBuilder()
                    .setDialResponse(Autonatv2.DialResponse.newBuilder()
                            .setStatus(status)
                            .setAddrIdx(addrIdx)
                            .setDialStatus(dialStatus))
                    .build());
        }
    }
}
