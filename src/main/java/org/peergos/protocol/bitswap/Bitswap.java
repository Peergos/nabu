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

    public CompletableFuture<byte[]> get(Cid hash, Host us) {
        return get(List.of(hash), us).get(0);
    }

    public List<CompletableFuture<byte[]>> get(List<Cid> hashes, Host us) {
        if (hashes.isEmpty())
            return Collections.emptyList();
        List<CompletableFuture<byte[]>> results = new ArrayList<>();
        for (Cid hash : hashes) {
            if (hash.getType() == Multihash.Type.id)
                continue;
            CompletableFuture<byte[]> res = engine.getWant(hash);
            results.add(res);
        }
        sendWants(us);
        ForkJoinPool.commonPool().execute(() -> {
            while (engine.hasWants()) {
                try {Thread.sleep(5_000);} catch (InterruptedException e) {}
                sendWants(us);
            }
        });
        return results;
    }

    public void sendWants(Host us) {
        LOG.info("Broadcast wants");
        Map<Cid, PeerId> haves = engine.getHaves();
        List<MessageOuterClass.Message.Wantlist.Entry> wants = engine.getWants().stream()
                .map(cid -> MessageOuterClass.Message.Wantlist.Entry.newBuilder()
                        .setWantType(haves.containsKey(cid) ?
                                MessageOuterClass.Message.Wantlist.WantType.Block :
                                MessageOuterClass.Message.Wantlist.WantType.Have)
                        .setBlock(ByteString.copyFrom(cid.toBytes()))
                        .build())
                .collect(Collectors.toList());
        // broadcast to all connected peers
        Set<PeerId> connected = engine.getConnected();
        engine.buildAndSendMessages(wants, Collections.emptyList(), Collections.emptyList(),
                msg -> connected.forEach(peer -> dialPeer(us, peer, c -> {
                    c.send(msg);
                    c.close();
                })));
    }

    private void dialPeer(Host us, PeerId peer, Consumer<BitswapController> action) {
        Multiaddr[] addr = addrs.get(peer).join().toArray(new Multiaddr[0]);
        BitswapController controller = dial(us, peer, addr).getController().join();
        action.accept(controller);
    }
}
