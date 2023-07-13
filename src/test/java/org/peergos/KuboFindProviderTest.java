package org.peergos;

import io.ipfs.api.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.mux.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.*;
import org.peergos.protocol.dht.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class KuboFindProviderTest {

    @Test
    public void findProviderOverYamux() throws IOException {
        HostBuilder builder1 = HostBuilder.create(TestPorts.getPort(), new RamProviderStore(),
                        new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addMuxers(List.of(StreamMuxerProtocol.getYamux()));
        Host node1 = builder1.build();
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1);
        try {
            IPFS kubo = new IPFS("localhost", 5001);
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kubo.id().get("ID"));
            KademliaController dht = builder1.getWanDht().get().dial(node1, address2).getController().join();
            Multihash block = Cid.decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
            Providers providers = dht.getProviders(block).orTimeout(2, TimeUnit.SECONDS).join();
            Assert.assertTrue(providers.closerPeers.size() == 20);
        } finally {
            node1.stop();
        }
    }
}
