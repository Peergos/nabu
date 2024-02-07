package org.peergos.protocol.dht;

import com.google.protobuf.*;
import com.offbynull.kademlia.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multiformats.Protocol;
import io.prometheus.client.*;
import org.peergos.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.*;
import org.peergos.util.*;

import java.io.*;
import java.net.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class KademliaEngine {

    private static final Counter responderReceivedBytes = Counter.build()
            .name("kademlia_responder_received_bytes")
            .help("Total received bytes in kademlia protocol responder")
            .register();
    private static final Counter responderSentBytes = Counter.build()
            .name("kademlia_responder_sent_bytes")
            .help("Total sent bytes in kademlia protocol responder")
            .register();
    private static final Counter responderIpnsSentBytes = Counter.build()
            .name("kademlia_responder_ipns_sent_bytes")
            .help("Total sent bytes in kademlia ipns protocol responder")
            .register();
    private static final Counter responderProvidersSentBytes = Counter.build()
            .name("kademlia_responder_providers_sent_bytes")
            .help("Total sent bytes in kademlia getProviders protocol responder")
            .register();
    private static final Counter responderFindNodeSentBytes = Counter.build()
            .name("kademlia_responder_find_node_sent_bytes")
            .help("Total sent bytes in kademlia findNode protocol responder")
            .register();

    private static final int BUCKET_SIZE = 20;
    private static final long REFRESH_PERIOD = 600_000_000_000L;
    private final ProviderStore providersStore;
    private final RecordStore ipnsStore;
    public final Router router;
    private AddressBook addressBook;
    private final Multihash ourPeerId;
    private final byte[] ourPeerIdBytes;
    private final Blockstore blocks;
    private final LinkedBlockingQueue<PeerId> toRefresh = new LinkedBlockingQueue<>();
    private final LRUCache<PeerId, Long> lastQueryTime = new LRUCache<>(1_000);

    public KademliaEngine(Multihash ourPeerId, ProviderStore providersStore, RecordStore ipnsStore, Blockstore blocks) {
        this.providersStore = providersStore;
        this.ipnsStore = ipnsStore;
        this.ourPeerId = ourPeerId;
        this.ourPeerIdBytes = ourPeerId.toBytes();
        this.router = new Router(Id.create(ourPeerId.bareMultihash().toBytes(), 256), 2, 2, 2);
        this.blocks = blocks;
    }

    public void setAddressBook(AddressBook addrs) {
        this.addressBook = addrs;
    }

    public synchronized void addOutgoingConnection(PeerId peer) {
        lastQueryTime.put(peer, System.nanoTime());
        addToRoutingTable(peer);
    }

    public synchronized void addIncomingConnection(PeerId peer) {
        // don't auto add incoming kademlia connections to routing table
    }

    public void markStale(PeerId peer) {
        router.stale(new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
    }

    public PeerId getPeerToRefresh() {
        return toRefresh.poll();
    }

    private void refresh(List<PeerAddresses> peers) {
        for (PeerAddresses peer : peers) {
            PeerId id = new PeerId(peer.peerId.toBytes());
            if (! lastQueryTime.containsKey(id) || lastQueryTime.get(id) < System.nanoTime() - REFRESH_PERIOD)
                toRefresh.add(id);
        }
    }

    private void addToRoutingTable(PeerId peer) {
        router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
    }

    public Set<PeerAddresses> getProviders(Multihash h) {
        return providersStore.getProviders(h)
                .stream()
                .map(PeerAddresses::fromProtobuf)
                .collect(Collectors.toSet());
    }

    public List<PeerAddresses> getKClosestPeers(byte[] key, int k) {
        List<Node> nodes;
        synchronized (this) {
            nodes = router.find(Id.create(Hash.sha256(key), 256), k, false);
        }
        return nodes.stream()
                .map(n -> {
                    List<Multiaddr> addrs = new ArrayList<>(addressBook.getAddrs(PeerId.fromBase58(n.getLink())).join());
                    return new PeerAddresses(Multihash.fromBase58(n.getLink()), addrs);
                })
                .filter(p -> ! p.addresses.isEmpty())
                .collect(Collectors.toList());
    }

    public void addRecord(Multihash publisher, IpnsRecord record) {
        ipnsStore.put(publisher, record);
    }

    public Optional<IpnsRecord> getRecord(Multihash publisher) {
        return ipnsStore.get(publisher);
    }

    public void receiveRequest(Dht.Message msg, PeerId source, Stream stream) {
        responderReceivedBytes.inc(msg.getSerializedSize());
        switch (msg.getType()) {
            case PUT_VALUE: {
                Optional<IpnsMapping> mapping = IPNS.parseAndValidateIpnsEntry(msg);
                if (mapping.isPresent()) {
                    Optional<IpnsRecord> existing = ipnsStore.get(mapping.get().publisher);
                    if (existing.isPresent() && mapping.get().value.compareTo(existing.get()) < 0) {
                        // don't add 'older' record
                        return;
                    }
                    ipnsStore.put(mapping.get().publisher, mapping.get().value);
                    stream.writeAndFlush(msg);
                    responderSentBytes.inc(msg.getSerializedSize());
                }
                break;
            }
            case GET_VALUE: {
                Cid key = IPNS.getCidFromKey(msg.getKey());
                Optional<IpnsRecord> ipnsRecord = ipnsStore.get(key);

                Dht.Message.Builder builder = msg.toBuilder();
                if (ipnsRecord.isPresent())
                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            .setValue(ByteString.copyFrom(ipnsRecord.get().raw)).build());
                builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray(), BUCKET_SIZE)
                        .stream()
                        .map(p -> p.toProtobuf(a -> isPublic(a)))
                        .collect(Collectors.toList()));
                Dht.Message reply = builder.build();
                stream.writeAndFlush(reply);
                responderSentBytes.inc(reply.getSerializedSize());
                responderIpnsSentBytes.inc(reply.getSerializedSize());
                break;
            }
            case ADD_PROVIDER: {
                List<Dht.Message.Peer> providers = msg.getProviderPeersList();
                byte[] remotePeerIdBytes = source.getBytes();
                Multihash hash = Multihash.deserialize(msg.getKey().toByteArray());
                if (providers.stream().allMatch(p -> Arrays.equals(p.getId().toByteArray(), remotePeerIdBytes))) {
                    providers.forEach(p -> providersStore.addProvider(hash, p));
                }
                break;
            }
            case GET_PROVIDERS: {
                Multihash hash = Multihash.deserialize(msg.getKey().toByteArray());
                Set<Dht.Message.Peer> providers = providersStore.getProviders(hash);
                if (blocks.hasAny(hash).join()) {
                    providers = new HashSet<>(providers);
                    providers.add(new PeerAddresses(ourPeerId,
                            addressBook.getAddrs(PeerId.fromBase58(ourPeerId.toBase58())).join()
                                    .stream()
                                    .filter(a -> isPublic(a))
                                    .collect(Collectors.toList())).toProtobuf());
                }
                Dht.Message.Builder builder = msg.toBuilder();
                builder = builder.addAllProviderPeers(providers.stream()
                        .collect(Collectors.toList()));
                builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray(), BUCKET_SIZE)
                        .stream()
                        .map(p -> p.toProtobuf(a -> isPublic(a)))
                        .collect(Collectors.toList()));
                Dht.Message reply = builder.build();
                stream.writeAndFlush(reply);
                responderSentBytes.inc(reply.getSerializedSize());
                responderProvidersSentBytes.inc(reply.getSerializedSize());
                break;
            }
            case FIND_NODE: {
                Dht.Message.Builder builder = msg.toBuilder();
                Multihash sourcePeer = Multihash.deserialize(source.getBytes());
                byte[] target = msg.getKey().toByteArray();
                if (Arrays.equals(target, ourPeerIdBytes)) {
                    // Only return ourselves (without addresses) if they are querying for us
                    // This is because all Go peers query for this to check live-ness
                    builder = builder.addCloserPeers(new PeerAddresses(ourPeerId, Collections.emptyList()).toProtobuf());
                } else {
                    List<PeerAddresses> kClosestPeers = getKClosestPeers(target, BUCKET_SIZE);
                    refresh(kClosestPeers);
                    builder = builder.addAllCloserPeers(kClosestPeers
                            .stream()
                            .filter(p -> ! p.peerId.equals(sourcePeer)) // don't tell a peer about themselves
                            .map(p -> p.toProtobuf(a -> isPublic(a)))
                            .collect(Collectors.toList()));
                }
                Dht.Message reply = builder.build();
                stream.writeAndFlush(reply);
                responderSentBytes.inc(reply.getSerializedSize());
                responderFindNodeSentBytes.inc(reply.getSerializedSize());
                break;
            }
            case PING: {break;} // Not used any more
            default: throw new IllegalStateException("Unknown message kademlia type: " + msg.getType());
        }
    }

    public static boolean isPublic(Multiaddr addr) {
        try {
            List<MultiaddrComponent> parts = addr.getComponents();
            for (MultiaddrComponent part: parts) {
                if (part.getProtocol() == Protocol.IP6ZONE)
                    return true;
                if (part.getProtocol() == Protocol.IP4 || part.getProtocol() == Protocol.IP6) {
                    InetAddress ip = InetAddress.getByName(part.getStringValue());
                    if (ip.isLoopbackAddress() || ip.isSiteLocalAddress() || ip.isLinkLocalAddress() || ip.isAnyLocalAddress())
                        return false;
                    return true;
                }
            }
        } catch (IOException e) {}
        return false;
    }
}
