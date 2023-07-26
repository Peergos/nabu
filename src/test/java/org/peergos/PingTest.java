package org.peergos;

import identify.pb.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.*;
import org.peergos.protocol.bitswap.*;

import java.util.*;
import java.util.concurrent.*;

public class PingTest {

    @Test
    public void runPing() {
        Host node1 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))));
        Host node2 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))));
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
    public void replyIdentifyOnNewDial() {
        Host node1 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))));
        Host node2 = HostBuilder.build(TestPorts.getPort(), List.of(new Ping(), new Bitswap(new BitswapEngine(new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true)))));
        node1.start().join();
        IdentifyBuilder.addIdentifyProtocol(node1);
        node2.start().join();
        IdentifyBuilder.addIdentifyProtocol(node2);
        try {
            // ping from 1 to 2
            Multiaddr address2 = node2.listenAddresses().get(0);
            PingController pinger1 = new Ping().dial(node1, address2).getController().join();
            pinger1.ping().join();

            // identify from 2 to 1
            Multiaddr address1 = node1.listenAddresses().get(0);
            IdentifyController id2 = new Identify().dial(node2, address1).getController().join();
            IdentifyOuterClass.Identify idRes = id2.id().join();
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
