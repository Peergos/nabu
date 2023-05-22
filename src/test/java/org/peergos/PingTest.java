package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.*;

import java.util.*;
import java.util.concurrent.*;

public class PingTest {

    @Test
    public void runPing() {
        Host node1 = HostBuilder.build(11001, List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))));
        Host node2 = HostBuilder.build(11002, List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))));
        node1.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            PingController pinger = new Ping().dial(node1, address2).getController().join();

            System.out.println("Sending ping messages to " + address2);
            for (int i = 0; i < 2; i++) {
                long latency = pinger.ping().join();
                System.out.println("Ping " + i + ", latency " + latency + "ms");
            }
        } finally {
            node1.stop();
            node2.stop();
        }
    }
    @Test
    public void runPingTest() {
        Host node2 = HostBuilder.build(11002, List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))));
        node2.start().join();
        try {
            //Multiaddr address = Multiaddr.fromString("/ip4/0.0.0.0/tcp/4001");
            Multiaddr address = Multiaddr.fromString("/ip4/127.0.0.1/tcp/4001");
            String peerIdStr = "12D3KooWA24mDfWkWRx2K3ERn6KKMfJbqxNjccVSQQdkCWUkjQZs"; //target.peerId.toBase58()
            PeerId peerId = PeerId.fromBase58(peerIdStr);
            PingController pinger = new Ping().dial(node2, peerId, address).getController().join();
            System.out.println("Sending ping messages to " + address);
            for (int i = 0; i < 2; i++) {
                long latency = pinger.ping().join();
                System.out.println("Ping " + i + ", latency " + latency + "ms");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            node2.stop();
        }
    }
}
