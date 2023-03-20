package org.peergos.protocol.bitswap;

import com.google.protobuf.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import org.peergos.*;
import org.peergos.protocol.bitswap.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;

public class Bitswap extends StrictProtocolBinding<BitswapController> implements AddressBookConsumer {
    private static final Logger LOG = Logger.getLogger(Bitswap.class.getName());
    public static int MAX_MESSAGE_SIZE = 2*1024*1024;

    private final BitswapEngine engine;
    private AddressBook addrs;

    public Bitswap(BitswapEngine engine) {
        super("/ipfs/bitswap/1.2.0", new BitswapProtocol(engine));
        this.engine = engine;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
        this.addrs = addrs;
    }

    public CompletableFuture<HashedBlock> get(Want hash,
                                              Host us,
                                              Set<PeerId> peers,
                                              boolean addToBlockstore) {
        return get(List.of(hash), us, peers, addToBlockstore).get(0);
    }

    public List<CompletableFuture<HashedBlock>> get(List<Want> wants,
                                                    Host us,
                                                    Set<PeerId> peers,
                                                    boolean addToBlockstore) {
        if (wants.isEmpty())
            return Collections.emptyList();
        List<CompletableFuture<HashedBlock>> results = new ArrayList<>();
        for (Want w : wants) {
            if (w.cid.getType() == Multihash.Type.id)
                continue;
            CompletableFuture<HashedBlock> res = engine.getWant(w, addToBlockstore);
            results.add(res);
        }
        sendWants(us, peers);
        ForkJoinPool.commonPool().execute(() -> {
            while (engine.hasWants()) {
                try {Thread.sleep(5_000);} catch (InterruptedException e) {}
                sendWants(us, peers);
            }
        });
        return results;
    }

    public void sendWants(Host us, Set<PeerId> peers) {
        Set<Want> wants = engine.getWants();
        LOG.info("Broadcast wants: " + wants.size());
        Map<Want, PeerId> haves = engine.getHaves();
        List<MessageOuterClass.Message.Wantlist.Entry> wantsProto = wants.stream()
                .map(want -> MessageOuterClass.Message.Wantlist.Entry.newBuilder()
                        .setWantType(haves.containsKey(want) ?
                                MessageOuterClass.Message.Wantlist.WantType.Block :
                                MessageOuterClass.Message.Wantlist.WantType.Have)
                        .setBlock(ByteString.copyFrom(want.cid.toBytes()))
                        .setAuth(ByteString.copyFrom(want.auth.orElse("").getBytes()))
                        .build())
                .collect(Collectors.toList());
        // broadcast to all connected peers if none are supplied
        Set<PeerId> connected = peers.isEmpty() ? engine.getConnected() : peers;
        engine.buildAndSendMessages(wantsProto, Collections.emptyList(), Collections.emptyList(),
                msg -> connected.forEach(peer -> dialPeer(us, peer, c -> {
                    c.send(msg);
                })));
    }

    private void dialPeer(Host us, PeerId peer, Consumer<BitswapController> action) {
        Multiaddr[] addr = addrs.get(peer).join().toArray(new Multiaddr[0]);
        if (addr.length == 0)
            throw new IllegalStateException("No addresses known for peer " + peer);
        BitswapController controller = dial(us, peer, addr).getController().join();
        action.accept(controller);
    }
}
