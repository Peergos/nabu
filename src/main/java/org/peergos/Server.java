package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;

public class Server {

    public static void main(String[] args) throws Exception {
        HostBuilder builder = HostBuilder.build(6001, new RamProviderStore(), new RamRecordStore(), new RamBlockstore());
        Host node1 = builder.build();
        node1.start().join();
        System.out.println("Node 1 started and listening on " + node1.listenAddresses());

        Multiaddr bootstrapNode = Multiaddr.fromString("/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt");
        KademliaController bootstrap = builder.getWanDht().get().dial(node1, bootstrapNode).getController().join();

        System.out.println("Stopping server...");
        node1.stop().get();
    }
}