package org.peergos.protocol.dht;

import com.google.protobuf.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import kotlin.*;
import org.peergos.*;
import org.peergos.protocol.dht.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public interface KademliaController {

    CompletableFuture<Dht.Message> rpc(Dht.Message msg);

    CompletableFuture<Boolean> send(Dht.Message msg);

    default CompletableFuture<List<PeerAddresses>> closerPeers(Cid peerID) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(peerID.toBytes()))
                .build())
                .thenApply(resp -> resp.getCloserPeersList().stream()
                        .map(PeerAddresses::fromProtobuf)
                        .collect(Collectors.toList()));
    }

    default CompletableFuture<Boolean> provide(Cid block, PeerAddresses us) {
        return send(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                // only provide the bare Multihash
                .setKey(ByteString.copyFrom(new Multihash(block.getType(), block.getHash()).toBytes()))
                .addAllProviderPeers(List.of(Dht.Message.Peer.newBuilder()
                        .setId(ByteString.copyFrom(us.peerId.toBytes()))
                        .addAllAddrs(us.addresses.stream()
                                .map(a -> ByteString.copyFrom(a.getBytes()))
                                .collect(Collectors.toList()))
                        .build()))
                .build());
    }

    default CompletableFuture<Providers> getProviders(Cid block) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(new Multihash(block.getType(), block.getHash()).toBytes()))
                .build())
                .thenApply(Providers::fromProtobuf);
    }

    void receive(Dht.Message msg);

    CompletableFuture<Unit> close();
}
