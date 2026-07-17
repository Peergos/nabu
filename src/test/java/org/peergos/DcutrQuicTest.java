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
 * End-to-end DCUtR over QUIC: a client reaches a server over a QUIC relay, then the two upgrade to a
 * direct QUIC connection via the hole-punch coordination. On loopback there is no NAT to punch, so this
 * exercises the QUIC hole-punch path (listen-port reuse dial + dialAsListener) and confirms a direct
 * (non-relayed) QUIC connection is established and usable.
 */
public class DcutrQuicTest {

    private static HostBuilder node(int port,
                                    Function<Host, List<CandidateRelay>> relays,
                                    Consumer<Connection> onUpgraded) {
        return new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/udp/" + port + "/quic-v1")))
                .addProtocols(List.of(new Ping()))
                .enableRelay(relays)
                .enableDcutr(onUpgraded);
    }

    @Test
    public void upgradeRelayedConnectionToDirect() throws Exception {
        HostBuilder relayBuilder = node(TestPorts.getPort(), h -> List.of(), c -> {});
        Host relay = relayBuilder.build();
        relay.start().join();
        relayBuilder.getReachability().setReachability(Reachability.PUBLIC, List.of());
        int relayPort = portOf(relay);
        List<Multiaddr> relayAddrs = List.of(new Multiaddr("/ip4/127.0.0.1/udp/" + relayPort + "/quic-v1"));
        Function<Host, List<CandidateRelay>> relayCandidate =
                h -> List.of(new CandidateRelay(relay.getPeerId(), relayAddrs));

        CompletableFuture<Connection> clientUpgraded = new CompletableFuture<>();
        Host server = node(TestPorts.getPort(), relayCandidate, c -> {}).build();
        HostBuilder clientBuilder = node(TestPorts.getPort(), relayCandidate, clientUpgraded::complete);
        Host client = clientBuilder.build();
        server.start().join();
        client.start().join();
        try {
            Multiaddr relayAddr = new Multiaddr("/ip4/127.0.0.1/udp/" + relayPort + "/quic-v1").withP2P(relay.getPeerId());
            // server reserves on the relay so the client can reach it
            server.getNetwork().listen(new Multiaddr(relayAddr + "/p2p-circuit")).get(15, TimeUnit.SECONDS);

            // client dials the server via the relay; the inbound side (server) initiates DCUtR
            Multiaddr toDial = relayAddr.concatenated(
                    new Multiaddr("/p2p-circuit/p2p/" + server.getPeerId().toBase58()));
            client.getNetwork().connect(server.getPeerId(), toDial).get(15, TimeUnit.SECONDS);

            // DCUtR should establish a direct (non-relayed) QUIC connection to the server
            Connection direct = clientUpgraded.get(20, TimeUnit.SECONDS);
            Assert.assertFalse("upgraded connection must be direct, not relayed: " + direct.remoteAddress(),
                    direct.remoteAddress().toString().contains("p2p-circuit"));
            Assert.assertTrue("upgraded connection must be QUIC: " + direct.remoteAddress(),
                    direct.remoteAddress().toString().contains("quic"));
            Assert.assertEquals("direct connection is to the server",
                    server.getPeerId(), direct.secureSession().getRemoteId());
            System.out.println("DCUtR upgraded to direct QUIC connection at " + direct.remoteAddress());

            // ping over the direct QUIC connection
            PingController pinger = direct.muxerSession().createStream(new Ping())
                    .getController().get(10, TimeUnit.SECONDS);
            long latency = pinger.ping().get(10, TimeUnit.SECONDS);
            System.out.println("Ping over DCUtR direct QUIC connection: " + latency + "ms");
        } finally {
            relay.stop();
            server.stop();
            client.stop();
        }
    }

    private static int portOf(Host h) {
        return Integer.parseInt(h.listenAddresses().stream()
                .map(Multiaddr::toString)
                .filter(s -> s.contains("/udp/"))
                .findFirst()
                .map(s -> s.substring(s.indexOf("/udp/") + 5).split("/")[0])
                .orElseThrow());
    }
}
