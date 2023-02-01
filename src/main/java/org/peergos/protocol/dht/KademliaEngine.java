package org.peergos.protocol.dht;

import com.google.protobuf.*;
import com.offbynull.kademlia.*;
import crypto.pb.*;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.crypto.keys.*;
import org.peergos.*;
import org.peergos.cbor.*;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.*;
import org.peergos.protocol.ipns.pb.*;

import java.io.*;
import java.nio.charset.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class KademliaEngine {

    private final Map<PeerId, KademliaController> outgoing = new ConcurrentHashMap<>();
    private final ProviderStore providersStore;
    private final RecordStore ipnsStore;
    public final Router router;
    private AddressBook addressBook;

    public KademliaEngine(ProviderStore providersStore, RecordStore ipnsStore) {
        this.providersStore = providersStore;
        this.ipnsStore = ipnsStore;
        this.router = new Router(Id.create(new byte[32], 256), 2, 2, 2);
    }

    public void setAddressBook(AddressBook addrs) {
        this.addressBook = addrs;
    }

    public void addOutgoingConnection(PeerId peer, KademliaController controller, Multiaddr addr) {
        if (outgoing.containsKey(peer))
            System.out.println("WARNING overwriting peer connection value");
        outgoing.put(peer, controller);
        router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
    }

    public void addIncomingConnection(PeerId peer, KademliaController controller, Multiaddr addr) {
        router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
    }

    public void receiveReply(Dht.Message msg, PeerId source) {
        KademliaController conn = outgoing.get(source);
        conn.receive(msg);
    }

    public boolean validateAndStoreIpnsEntry(Dht.Message msg, boolean addToStore) {
        if (! msg.hasRecord() || msg.getRecord().getValue().size() > IPNS.MAX_RECORD_SIZE)
            return false;
        if (! msg.getKey().equals(msg.getRecord().getKey()))
            return false;
        if (! msg.getRecord().getKey().startsWith(ByteString.copyFrom("/ipns/".getBytes(StandardCharsets.UTF_8))))
            return false;
        byte[] cidBytes = msg.getRecord().getKey().substring(6).toByteArray();
        Cid signer = Cid.cast(cidBytes);
        try {
            Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
            if (! entry.hasSignatureV2() || ! entry.hasData())
                return false;
            PubKey pub;
            if (signer.getType() == Multihash.Type.id) {
                pub = new Ed25519PublicKey(new org.bouncycastle.crypto.params.Ed25519PublicKeyParameters(signer.getHash(), 0));
            } else {
                Crypto.PublicKey publicKey = Crypto.PublicKey.parseFrom(entry.getPubKey());
                pub = RsaKt.unmarshalRsaPublicKey(publicKey.getData().toByteArray());
            }
            if (! pub.verify(entry.getSignatureV2().toByteArray(),
                    ByteString.copyFrom("ipns-signature:".getBytes()).concat(entry.getData()).toByteArray()))
                return false;
            CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
            if (! (cbor instanceof CborObject.CborMap))
                return false;
            CborObject.CborMap map = (CborObject.CborMap) cbor;
            if (map.getLong("Sequence") != entry.getSequence())
                return false;
            if (map.getLong("TTL") != entry.getTtl())
                return false;
            if (map.getLong("ValidityType") != entry.getValidityType().getNumber())
                return false;
            if (! Arrays.equals(map.getByteArray("Value"), entry.getValue().toByteArray()))
                return false;
            byte[] validity = entry.getValidity().toByteArray();
            if (! Arrays.equals(map.getByteArray("Validity"), validity))
                return false;
            LocalDateTime expiry = LocalDateTime.parse(new String(validity).substring(0, validity.length - 1), IPNS.rfc3339nano);
            if (expiry.isBefore(LocalDateTime.now()))
                return false;
            if (addToStore) {
                byte[] entryBytes = msg.getRecord().getValue().toByteArray();
                ipnsStore.put(signer, new IpnsRecord(entryBytes, entry.getSequence(), entry.getTtl(), expiry, entry.getValue().toStringUtf8()));
            }
            return true;
        } catch (InvalidProtocolBufferException e) {
            return false;
        }
    }

    public List<PeerAddresses> getKClosestPeers(byte[] key) {
        int k = 20;
        List<Node> nodes = router.find(Id.create(Hash.sha256(key), 256), k, false);
        return nodes.stream()
                .map(n -> {
                    List<MultiAddress> addrs = addressBook.getAddrs(PeerId.fromBase58(n.getLink())).join()
                            .stream()
                            .map(m -> new MultiAddress(m.toString()))
                            .collect(Collectors.toList());
                    return new PeerAddresses(Multihash.decode(n.getLink()), addrs);
                })
                .collect(Collectors.toList());
    }

    public void receiveRequest(Dht.Message msg, PeerId source, Stream stream) {
        switch (msg.getType()) {
            case PUT_VALUE: {
                if (validateAndStoreIpnsEntry(msg, true)) {
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
