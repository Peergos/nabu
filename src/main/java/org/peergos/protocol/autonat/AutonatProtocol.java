package org.peergos.protocol.autonat;

import com.google.protobuf.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;
import org.peergos.*;
import org.peergos.protocol.autonat.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class AutonatProtocol extends ProtobufProtocolHandler<AutonatProtocol.AutoNatController> {

    public static class Binding extends StrictProtocolBinding<AutoNatController> {
        public Binding() {
            super("/libp2p/autonat/v1.0.0", new AutonatProtocol());
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
                                            .map(a -> ByteString.copyFrom(a.getBytes()))
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

    public static boolean isPublicAndReachable(MultiAddress addr, Multiaddr source) {
        if (addr.isRelayed())
            return false;

       return addr.isPublic(true);
    }

    public static class Receiver implements ProtocolMessageHandler<Autonat.Message>, AutoNatController {
        private final Stream p2pstream;

        public Receiver(Stream p2pstream) {
            this.p2pstream = p2pstream;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Autonat.Message msg) {
            switch (msg.getType()) {
                case DIAL: {
                    Autonat.Message.Dial dial = msg.getDial();
                    Multihash peerId = Multihash.deserialize(dial.getPeer().getId().toByteArray());
                    List<MultiAddress> requestedDials = dial.getPeer().getAddrsList().stream()
                            .map(b -> new MultiAddress(b.toByteArray()))
                            .collect(Collectors.toList());
                    PeerId streamPeerId = stream.remotePeerId();
                    if (! Arrays.equals(peerId.toBytes(), streamPeerId.getBytes())) {
                        p2pstream.close();
                        return;
                    }

                    Multiaddr remote = stream.getConnection().remoteAddress();
                    boolean reachable = requestedDials.stream()
                            .filter(a -> isPublicAndReachable(a, remote))
                            .findAny()
                            .isPresent();
                    Autonat.Message.Builder resp = Autonat.Message.newBuilder()
                            .setType(Autonat.Message.MessageType.DIAL_RESPONSE);
                    if (reachable) {
                        resp = resp.setDialResponse(Autonat.Message.DialResponse.newBuilder()
                                .setStatus(Autonat.Message.ResponseStatus.OK));
                    } else {
                        resp = resp.setDialResponse(Autonat.Message.DialResponse.newBuilder()
                                .setStatus(Autonat.Message.ResponseStatus.E_DIAL_ERROR));
                    }
                    p2pstream.writeAndFlush(resp);
                }
                default: {}
            }
        }

        public CompletableFuture<Autonat.Message> rpc(Autonat.Message msg) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot send form a receiver!"));
        }
    }

    private static final int TRAFFIC_LIMIT = 2*1024;

    public AutonatProtocol() {
        super(Autonat.Message.getDefaultInstance(), TRAFFIC_LIMIT, TRAFFIC_LIMIT);
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
        Receiver dialer = new Receiver(stream);
        stream.pushHandler(dialer);
        return CompletableFuture.completedFuture(dialer);
    }
}
