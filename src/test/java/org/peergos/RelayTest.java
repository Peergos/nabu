package org.peergos;

import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import io.libp2p.protocol.circuit.RelayTransport.CandidateRelay;
import org.junit.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * End-to-end circuit-relay-v2 data path through nabu's HostBuilder, using the upstream jvm-libp2p
 * relay implementation: a NATed server reserves on a public relay, and a client reaches it over
 * /p2p-circuit and pings it - exercising reservation, the relay byte splice, and the relay transport.
 */
public class RelayTest {

    private static HostBuilder relayEnabled(int port) {
        return relayEnabled(port, h -> List.of());
    }

    private static HostBuilder relayEnabled(int port, Function<Host, List<CandidateRelay>> candidateSource) {
        HostBuilder builder = new HostBuilder(new RamAddressBook()).generateIdentity();
        if (port > 0)
            builder.listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + port)));
        return builder.addProtocols(List.of(new Ping())).enableRelay(candidateSource);
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

    @Test
    public void autoTriggerReservesRelayWhenPrivate() throws Exception {
        HostBuilder relayBuilder = relayEnabled(TestPorts.getPort());
        Host relay = relayBuilder.build();
        relay.start().join();
        relayBuilder.getReachability().setReachability(Reachability.PUBLIC, List.of());

        int relayPort = portOf(relay);
        List<Multiaddr> relayAddrs = List.of(new Multiaddr("/ip4/127.0.0.1/tcp/" + relayPort));
        // the server only knows about this relay as a candidate; AutoRelay must reserve on it
        Function<Host, List<CandidateRelay>> serverRelays =
                h -> List.of(new CandidateRelay(relay.getPeerId(), relayAddrs));

        HostBuilder serverBuilder = relayEnabled(TestPorts.getPort(), serverRelays);
        HostBuilder clientBuilder = relayEnabled(TestPorts.getPort());
        Host server = serverBuilder.build();
        Host client = clientBuilder.build();
        server.start().join();
        client.start().join();
        try {
            // Going PRIVATE should make AutoRelay reserve a slot on the candidate relay (async)
            serverBuilder.getReachability().setReachability(Reachability.PRIVATE, List.of());

            // wait until the server advertises a relayed address (i.e. the reservation succeeded)
            Multiaddr circuitAddr = waitForRelayedAddress(server, 15_000);
            Assert.assertNotNull("server should advertise a /p2p-circuit address once reserved", circuitAddr);
            Assert.assertTrue("relayed address is announceable via listenAddresses: " + circuitAddr,
                    circuitAddr.toString().contains("p2p-circuit"));

            Multiaddr relayAddr = new Multiaddr("/ip4/127.0.0.1/tcp/" + relayPort).withP2P(relay.getPeerId());
            Multiaddr toDial = relayAddr.concatenated(
                    new Multiaddr("/p2p-circuit/p2p/" + server.getPeerId().toBase58()));
            Connection conn = client.getNetwork().connect(server.getPeerId(), toDial).get(15, TimeUnit.SECONDS);

            PingController pinger = conn.muxerSession().createStream(new Ping())
                    .getController().get(15, TimeUnit.SECONDS);
            long latency = pinger.ping().get(15, TimeUnit.SECONDS);
            System.out.println("Ping over auto-reserved relay latency: " + latency + "ms");
        } finally {
            relay.stop();
            server.stop();
            client.stop();
        }
    }

    private static Multiaddr waitForRelayedAddress(Host host, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            Optional<Multiaddr> relayed = host.listenAddresses().stream()
                    .filter(a -> a.toString().contains("p2p-circuit"))
                    .findFirst();
            if (relayed.isPresent())
                return relayed.get();
            Thread.sleep(100);
        }
        return null;
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
