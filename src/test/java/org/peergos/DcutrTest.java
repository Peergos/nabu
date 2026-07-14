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
 * End-to-end DCUtR: a client reaches a server over a relay, then the two upgrade to a direct connection
 * via the hole-punch coordination. On loopback there is no NAT to punch, so this exercises the protocol
 * (CONNECT/SYNC exchange, roles, timing) and confirms a direct (non-relayed) connection is established.
 */
public class DcutrTest {

    private static HostBuilder node(int port,
                                    Function<Host, List<CandidateRelay>> relays,
                                    Consumer<Connection> onUpgraded) {
        return new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + port)))
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
        List<Multiaddr> relayAddrs = List.of(new Multiaddr("/ip4/127.0.0.1/tcp/" + relayPort));
        Function<Host, List<CandidateRelay>> relayCandidate =
                h -> List.of(new CandidateRelay(relay.getPeerId(), relayAddrs));

        CompletableFuture<Connection> clientUpgraded = new CompletableFuture<>();
        Host server = node(TestPorts.getPort(), relayCandidate, c -> {}).build();
        HostBuilder clientBuilder = node(TestPorts.getPort(), relayCandidate, clientUpgraded::complete);
        Host client = clientBuilder.build();
        server.start().join();
        client.start().join();
        try {
            Multiaddr relayAddr = new Multiaddr("/ip4/127.0.0.1/tcp/" + relayPort).withP2P(relay.getPeerId());
            // server reserves on the relay so the client can reach it
            server.getNetwork().listen(new Multiaddr(relayAddr + "/p2p-circuit")).get(15, TimeUnit.SECONDS);

            // client dials the server via the relay; the inbound side (server) initiates DCUtR
            Multiaddr toDial = relayAddr.concatenated(
                    new Multiaddr("/p2p-circuit/p2p/" + server.getPeerId().toBase58()));
            client.getNetwork().connect(server.getPeerId(), toDial).get(15, TimeUnit.SECONDS);

            // DCUtR should establish a direct (non-relayed) connection to the server
            Connection direct = clientUpgraded.get(20, TimeUnit.SECONDS);
            Assert.assertFalse("upgraded connection must be direct, not relayed: " + direct.remoteAddress(),
                    direct.remoteAddress().toString().contains("p2p-circuit"));
            Assert.assertEquals("direct connection is to the server",
                    server.getPeerId(), direct.secureSession().getRemoteId());
            System.out.println("DCUtR upgraded to direct connection at " + direct.remoteAddress());
        } finally {
            relay.stop();
            server.stop();
            client.stop();
        }
    }

    private static int portOf(Host h) {
        return Integer.parseInt(h.listenAddresses().stream()
                .map(Multiaddr::toString)
                .filter(s -> s.contains("/tcp/"))
                .findFirst()
                .map(s -> s.substring(s.indexOf("/tcp/") + 5).split("/")[0])
                .orElseThrow());
    }
}
