package org.peergos.protocol.dht;

import com.google.protobuf.*;
import io.ipfs.cid.*;
import kotlin.*;
import org.peergos.*;
import org.peergos.protocol.dht.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public interface KademliaController {

    CompletableFuture<Dht.Message> send(Dht.Message msg);

    default CompletableFuture<List<PeerAddresses>> closerPeers(Cid peerID) {
        return send(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(peerID.toBytes()))
                .build())
                .thenApply(resp -> resp.getCloserPeersList().stream()
                        .map(PeerAddresses::fromProtobuf)
                        .collect(Collectors.toList()));
    }

    void receive(Dht.Message msg);

    CompletableFuture<Unit> close();
}
