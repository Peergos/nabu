package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.bitswap.*;

import java.util.*;

public class PingTest {

    @Test
    public void runPing() {
        Host node1 = Server.buildHost(11001, new Bitswap(new BitswapEngine(new RamBlockstore())), Optional.empty());
        Host node2 = Server.buildHost(11002, new Bitswap(new BitswapEngine(new RamBlockstore())), Optional.empty());
        node1.start().join();
        node2.start().join();
        try {
            Multiaddr address2 = node2.listenAddresses().get(0);
            PingController pinger = new Ping().dial(node1, address2).getController().join();

            System.out.println("Sending 5 ping messages to " + address2);
            for (int i = 0; i < 2; i++) {
                long latency = pinger.ping().join();
                System.out.println("Ping " + i + ", latency " + latency + "ms");
            }
        } finally {
            node1.stop();
            node2.stop();
        }
    }
}
