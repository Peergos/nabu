package org.peergos;

import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.concurrent.*;

/**
 * End-to-end circuit-relay-v2 data path through nabu's HostBuilder, using the upstream jvm-libp2p
 * relay implementation: a NATed server reserves on a public relay, and a client reaches it over
 * /p2p-circuit and pings it - exercising reservation, the relay byte splice, and the relay transport.
 */
public class RelayTest {

    private static HostBuilder relayEnabled(int port) {
        HostBuilder builder = new HostBuilder(new RamAddressBook()).generateIdentity();
        if (port > 0)
            builder.listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + port)));
        return builder.addProtocols(List.of(new Ping())).enableRelay(h -> List.of());
    }

    @Test
    public void pingOverRelay() throws Exception {
        HostBuilder relayBuilder = relayEnabled(TestPorts.getPort());
        int serverPort = TestPorts.getPort();
        HostBuilder serverBuilder = relayEnabled(serverPort);
        HostBuilder clientBuilder = relayEnabled(TestPorts.getPort());

        Host relay = relayBuilder.build();
        Host server = serverBuilder.build();
        Host client = clientBuilder.build();
        relay.start().join();
        server.start().join();
        client.start().join();
        // the relay is publicly reachable, so it will grant reservations
        relayBuilder.getReachability().setReachability(Reachability.PUBLIC, List.of());
        try {
            Multiaddr relayAddr = new Multiaddr("/ip4/127.0.0.1/tcp/" + portOf(relay))
                    .withP2P(relay.getPeerId());

            // NATed server reserves a slot on the relay so it can be reached through it
            server.getNetwork().listen(new Multiaddr(relayAddr + "/p2p-circuit")).get(15, TimeUnit.SECONDS);

            // client dials the server via the relay and pings it
            Multiaddr toDial = relayAddr.concatenated(
                    new Multiaddr("/p2p-circuit/p2p/" + server.getPeerId().toBase58()));
            Connection conn = client.getNetwork().connect(server.getPeerId(), toDial).get(15, TimeUnit.SECONDS);
            Assert.assertTrue("connection is over the relay transport",
                    conn.remoteAddress().toString().contains("p2p-circuit"));

            StreamPromise<PingController> ping = conn.muxerSession().createStream(new Ping());
            PingController pinger = ping.getController().get(15, TimeUnit.SECONDS);
            long latency = pinger.ping().get(15, TimeUnit.SECONDS);
            System.out.println("Ping over relay latency: " + latency + "ms");
        } finally {
            relay.stop();
            server.stop();
            client.stop();
        }
    }

    private static int portOf(Host h) {
        // the tcp listen address we bound to
        return Integer.parseInt(h.listenAddresses().stream()
                .map(Multiaddr::toString)
                .filter(s -> s.contains("/tcp/"))
                .findFirst()
                .map(s -> s.substring(s.indexOf("/tcp/") + 5).split("/")[0])
                .orElseThrow());
    }
}
