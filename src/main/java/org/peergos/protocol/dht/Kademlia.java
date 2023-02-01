package org.peergos.protocol.dht;

import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class Kademlia extends StrictProtocolBinding<KademliaController> {
    private final KademliaEngine engine;

    public Kademlia(KademliaEngine dht, boolean localOnly) {
        super("/ipfs/" + (localOnly ? "lan/" : "") + "kad/1.0.0", new KademliaProtocol(dht));
        this.engine = dht;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
    }

    public int bootstrapRoutingTable(Host host, List<MultiAddress> addrs) {
        List<? extends CompletableFuture<? extends KademliaController>> futures = addrs.stream()
                .parallel()
                .map(addr -> dial(host, Multiaddr.fromString(addr.toString())).getController())
                .collect(Collectors.toList());
        int successes = 0;
        for (CompletableFuture<? extends KademliaController> future : futures) {
            try {
                future.orTimeout(5, TimeUnit.SECONDS).join();
                successes++;
            } catch (Exception e) {}
        }
        return successes;
    }
}
