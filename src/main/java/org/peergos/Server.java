package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.dsl.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;

public class Server {

    public static void main(String[] args) throws Exception {
        // Create a libp2p node and configure it
        // to accept TCP connections on a random port
        Host node = new HostBuilder()
                .protocol(new Ping())
                .listen("/ip4/127.0.0.1/tcp/0")
                .build();

        // start listening
        node.start().get();

        System.out.print("Node started and listening on ");
        System.out.println(node.listenAddresses());

        // start a second node
        Host node2 = new HostBuilder()
                .protocol(new Ping())
                .listen("/ip4/127.0.0.1/tcp/0")
                .build();

        // start listening
        node2.start().get();

        // ping between
        Multiaddr address = node2.listenAddresses().get(0);
        PingController pinger = new Ping().dial(
                node,
                address
        ).getController().get();

        System.out.println("Sending 5 ping messages to " + address);
        for (int i = 1; i <= 5; ++i) {
            long latency = pinger.ping().get();
            System.out.println("Ping " + i + ", latency " + latency + "ms");
        }

        node.stop().get();
        node2.stop().get();
    }
}