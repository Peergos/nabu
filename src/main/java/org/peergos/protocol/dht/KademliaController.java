package org.peergos.protocol.dht;

import com.google.protobuf.*;
import crypto.pb.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import io.libp2p.core.crypto.*;
import kotlin.*;
import org.peergos.*;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.*;
import org.peergos.protocol.ipns.pb.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public interface KademliaController {

    CompletableFuture<Dht.Message> rpc(Dht.Message msg);

    CompletableFuture<Boolean> send(Dht.Message msg);

    default CompletableFuture<List<PeerAddresses>> closerPeers(Multihash peerID) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(peerID.toBytes()))
                .build())
                .thenApply(resp -> resp.getCloserPeersList().stream()
                        .map(PeerAddresses::fromProtobuf)
                        .collect(Collectors.toList()));
    }

    default CompletableFuture<Boolean> provide(Multihash block, PeerAddresses us) {
        return send(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                // only provide the bare Multihash
                .setKey(ByteString.copyFrom(block.bareMultihash().toBytes()))
                .addAllProviderPeers(List.of(us.toProtobuf()))
                .build());
    }

    default CompletableFuture<Providers> getProviders(Multihash block) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(block.bareMultihash().toBytes()))
                .build())
                .thenApply(Providers::fromProtobuf);
    }

    default CompletableFuture<Boolean> putValue(String pathToPublish, LocalDateTime expiry, long sequence,
                                                long ttlNanos, Multihash peerId, PrivKey ourKey) {
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();
        return putValue(peerId, ipnsEntry);
    }

    default CompletableFuture<Boolean> putValue(Multihash peerId, byte[] value) {
        byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.PUT_VALUE)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(value))
                        .build())
                .build();
        return rpc(outgoing).thenApply(reply -> reply.getKey().equals(outgoing.getKey()) &&
                reply.getRecord().getValue().equals(outgoing.getRecord().getValue()));
    }

    default CompletableFuture<GetResult> getValue(Multihash peerId) {
        byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .build();
        return rpc(outgoing).thenApply(GetResult::fromProtobuf);
    }

    void receive(Dht.Message msg);

    CompletableFuture<Unit> close();
}
