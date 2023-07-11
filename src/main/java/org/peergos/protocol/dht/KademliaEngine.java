package org.peergos.protocol.dht;

import com.google.protobuf.*;
import com.offbynull.kademlia.*;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import org.peergos.*;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.*;

import java.time.*;
import java.util.*;
import java.util.stream.*;

public class KademliaEngine {

    private final ProviderStore providersStore;
    private final RecordStore ipnsStore;
    public final Router router;
    private AddressBook addressBook;

    public KademliaEngine(Multihash ourPeerId, ProviderStore providersStore, RecordStore ipnsStore) {
        this.providersStore = providersStore;
        this.ipnsStore = ipnsStore;
        this.router = new Router(Id.create(ourPeerId.bareMultihash().toBytes(), 256), 2, 2, 2);
    }

    public void setAddressBook(AddressBook addrs) {
        this.addressBook = addrs;
    }

    public synchronized void addOutgoingConnection(PeerId peer, Multiaddr addr) {
        router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
        addressBook.addAddrs(peer, 0, addr);
    }

    public synchronized void addIncomingConnection(PeerId peer, Multiaddr addr) {
        router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
        addressBook.addAddrs(peer, 0, addr);
    }

    public Set<PeerAddresses> getProviders(Multihash h) {
        return providersStore.getProviders(h);
    }

    public List<PeerAddresses> getKClosestPeers(byte[] key) {
        int k = 20;
        List<Node> nodes;
        synchronized (this) {
            nodes = router.find(Id.create(Hash.sha256(key), 256), k, false);
        }
        System.out.println("Nodes: " + nodes.size());
        return nodes.stream()
                .map(n -> {
                    List<MultiAddress> addrs = addressBook.getAddrs(PeerId.fromBase58(n.getLink())).join()
                            .stream()
                            .map(m -> new MultiAddress(m.toString()))
                            .collect(Collectors.toList());
                    return new PeerAddresses(Multihash.fromBase58(n.getLink()), addrs);
                })
                .collect(Collectors.toList());
    }

    public void receiveRequest(Dht.Message msg, PeerId source, Stream stream) {
        System.out.println("Received: " + msg.getType());
        switch (msg.getType()) {
            case PUT_VALUE: {
                Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);
                if (mapping.isPresent()) {
                    ipnsStore.put(mapping.get().publisher, mapping.get().value);
                    stream.writeAndFlush(msg);
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
                builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray())
                        .stream()
                        .map(PeerAddresses::toProtobuf)
                        .collect(Collectors.toList()));
                stream.writeAndFlush(builder.build());
                break;
            }
            case ADD_PROVIDER: {
                List<Dht.Message.Peer> providers = msg.getProviderPeersList();
                byte[] remotePeerIdBytes = source.getBytes();
                Multihash hash = Multihash.deserialize(msg.getKey().toByteArray());
                if (providers.stream().allMatch(p -> Arrays.equals(p.getId().toByteArray(), remotePeerIdBytes))) {
                    providers.stream().map(PeerAddresses::fromProtobuf).forEach(p -> providersStore.addProvider(hash, p));
                }
                break;
            }
            case GET_PROVIDERS: {
                Multihash hash = Multihash.deserialize(msg.getKey().toByteArray());
                Set<PeerAddresses> providers = providersStore.getProviders(hash);
                Dht.Message.Builder builder = msg.toBuilder();
                builder = builder.addAllProviderPeers(providers.stream()
                        .map(PeerAddresses::toProtobuf)
                        .collect(Collectors.toList()));
                builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray())
                        .stream()
                        .map(PeerAddresses::toProtobuf)
                        .collect(Collectors.toList()));
                stream.writeAndFlush(builder.build());
                break;
            }
            case FIND_NODE: {
                Dht.Message.Builder builder = msg.toBuilder();
                builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray())
                        .stream()
                        .map(PeerAddresses::toProtobuf)
                        .collect(Collectors.toList()));
                stream.writeAndFlush(builder.build());
                break;
            }
            case PING: {break;} // Not used any more
            default: throw new IllegalStateException("Unknown message kademlia type: " + msg.getType());
        }
    }
}
