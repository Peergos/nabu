package org.peergos.protocol.bitswap;

import com.google.protobuf.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import org.jetbrains.annotations.*;
import org.peergos.*;
import org.peergos.protocol.bitswap.pb.*;
import org.peergos.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;

public class Bitswap extends StrictProtocolBinding<BitswapController> implements AddressBookConsumer, ConnectionHandler {
    private static final Logger LOG = Logging.LOG();
    public static int MAX_MESSAGE_SIZE = 2*1024*1024;
    public static final String PROTOCOL_ID = "/ipfs/bitswap/1.2.0";

    private final BitswapEngine engine;
    private final LRUCache<PeerId, Boolean> connected = new LRUCache<>(100);
    private final LRUCache<Set<PeerId>, DownloadManager> downloads = new LRUCache<>(100);
    private AddressBook addrs;

    public Bitswap(BitswapEngine engine) {
        super(PROTOCOL_ID, new BitswapProtocol(engine));
        this.engine = engine;
    }

    public Bitswap(String protocolId, BitswapEngine engine) {
        super(protocolId, new BitswapProtocol(engine));
        this.engine = engine;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
        this.addrs = addrs;
    }

    public int maxBlockSize() {
        return engine.maxMessageSize();
    }

    @Override
    public void handleConnection(@NotNull Connection connection) {
        // add all outgoing connections to an LRU of candidates
        if (connection.isInitiator()) {
            PeerId remoteId = connection.secureSession().getRemoteId();
            connected.put(remoteId, true);
        }
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
        DownloadManager manager = downloads.getOrDefault(peers, new DownloadManager(us, peers));
        manager.ensureRunning();
        return results;
    }

    public Set<PeerId> getBroadcastAudience() {
        HashSet<PeerId> res = new HashSet<>(engine.getConnected());
        res.addAll(connected.keySet());
        return res;
    }

    private class DownloadManager {
        private final Host us;
        private final Set<PeerId> peers;
        private final AtomicBoolean running = new AtomicBoolean(false);

        public DownloadManager(Host us, Set<PeerId> peers) {
            this.us = us;
            this.peers = peers;
        }

        public void ensureRunning() {
            if (! running.get())
                new Thread(() -> run()).start();
        }

        public void run() {
            running.set(true);
            while (true) {
                try {Thread.sleep(5_000);} catch (InterruptedException e) {}
                Set<Want> wants = engine.getWants(peers);
                if (wants.isEmpty())
                    break;
                sendWants(us, wants, peers);
            }
            running.set(false);
        }
    }

    public void sendWants(Host us, Set<PeerId> peers) {
        Set<Want> wants = engine.getWants(peers);
        sendWants(us, wants, peers);
    }

    public void sendWants(Host us, Set<Want> wants, Set<PeerId> peers) {
        Map<Want, PeerId> haves = engine.getHaves();
        // broadcast to all connected bitswap peers if none are supplied
        Set<PeerId> audience = peers.isEmpty() ? getBroadcastAudience() : peers;
        LOG.info("Send wants: " + wants.size() + " to " + audience);
        List<MessageOuterClass.Message.Wantlist.Entry> wantsProto = wants.stream()
                .map(want -> MessageOuterClass.Message.Wantlist.Entry.newBuilder()
                        .setWantType(audience.size() <= 2 || haves.containsKey(want) ?
                                MessageOuterClass.Message.Wantlist.WantType.Block :
                                MessageOuterClass.Message.Wantlist.WantType.Have)
                        .setBlock(ByteString.copyFrom(want.cid.toBytes()))
                        .setAuth(ByteString.copyFrom(ArrayOps.hexToBytes(want.authHex.orElse(""))))
                        .build())
                .collect(Collectors.toList());
        engine.buildAndSendMessages(wantsProto, Collections.emptyList(), Collections.emptyList(),
                msg -> audience.forEach(peer -> {
                    try {
                        dialPeer(us, peer, c -> {
                            c.send(msg);
                        });
                    } catch (Exception e) {}
                }));
    }

    private void dialPeer(Host us, PeerId peer, Consumer<BitswapController> action) {
        Multiaddr[] addr = addrs.get(peer).join().toArray(new Multiaddr[0]);
        if (addr.length == 0)
            throw new IllegalStateException("No addresses known for peer " + peer);
        BitswapController controller = dial(us, peer, addr).getController().join();
        action.accept(controller);
    }

    public class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int cacheSize;

        public LRUCache(int cacheSize) {
            super(16, 0.75f, true);
            this.cacheSize = cacheSize;
        }

        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() >= cacheSize;
        }
    }
}
