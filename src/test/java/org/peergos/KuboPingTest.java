package org.peergos;

import io.ipfs.api.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.mux.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class KuboPingTest {

    @Test
    public void runPingOverYamux() throws IOException {
        Host node1 = new HostBuilder()
                .generateIdentity()
                .listenLocalhost(11001)
                .addProtocols(List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))))
                .addMuxers(List.of(StreamMuxerProtocol.getYamux()))
                .build();
        node1.start().join();
        try {
            IPFS kubo = new IPFS("localhost", 5001);
            Multiaddr address2 = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001/p2p/" + kubo.id().get("ID"));
            PingController pinger = new Ping().dial(node1, address2).getController().join();

            System.out.println("Sending ping messages to " + address2);
            for (int i = 0; i < 10_000; i++) { // 8091 is where we hit the yamux 256kb window size, so do more than that
                long latency = pinger.ping().join();
                if (i % 10 == 0)
                    System.out.println("Ping " + i + ", latency " + latency + "ms");
            }
        } finally {
            node1.stop();
        }
    }
}
