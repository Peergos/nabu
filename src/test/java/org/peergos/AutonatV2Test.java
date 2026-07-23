package org.peergos;

import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.concurrent.*;

/**
 * End-to-end AutoNAT v2: a client asks a server to dial one of its addresses; the server dials it back on
 * a fresh connection and proves it by echoing the nonce over the dial-back stream, and the client
 * concludes the address is reachable.
 */
public class AutonatV2Test {

    @Test
    public void verifyReachabilityViaDialBack() throws Exception {
        int portA = TestPorts.getPort();
        int portB = TestPorts.getPort();

        // client A, with an explicit nonce registry so we can drive its v2 client
        NonceRegistry noncesA = new NonceRegistry();
        AutonatV2DialBack.Binding dialBackA = new AutonatV2DialBack.Binding(noncesA);
        AutonatV2.Binding v2A = new AutonatV2.Binding(dialBackA);
        HostBuilder builderA = new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + portA)))
                .addProtocols(List.of(new Ping(), v2A, dialBackA));
        Host clientHost = builderA.build();

        // server B is a plain v2 responder
        List<ProtocolBinding> serverProtocols = new ArrayList<>(List.of(new Ping()));
        serverProtocols.addAll(AutonatV2.protocols());
        Host serverHost = new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + portB)))
                .addProtocols(serverProtocols)
                .build();

        clientHost.start().join();
        serverHost.start().join();
        try {
            Connection conn = clientHost.getNetwork()
                    .connect(serverHost.getPeerId(), new Multiaddr("/ip4/127.0.0.1/tcp/" + portB))
                    .get(15, TimeUnit.SECONDS);

            Multiaddr ourAddr = new Multiaddr("/ip4/127.0.0.1/tcp/" + portA);
            ReachabilityManager reachability = builderA.getReachability();
            AutoNatV2Client client = new AutoNatV2Client(clientHost, reachability, v2A, noncesA,
                    () -> List.of(ourAddr), 1);
            client.onConnection(conn);

            for (int i = 0; i < 100 && reachability.getReachability() != Reachability.PUBLIC; i++)
                Thread.sleep(100);
            Assert.assertEquals("server verified our address is dialable",
                    Reachability.PUBLIC, reachability.getReachability());
            Assert.assertTrue("the verified address is recorded as public",
                    reachability.getConfirmedPublicAddresses().contains(ourAddr));
            System.out.println("AutoNAT v2 confirmed reachable at " + ourAddr);
        } finally {
            clientHost.stop();
            serverHost.stop();
        }
    }

    /**
     * A verdict of PRIVATE reached from an early (unreachable) candidate must not stick once a genuinely
     * reachable candidate appears: probePending re-opens probing and flips us to PUBLIC, rather
     * than leaving us wrongly PRIVATE until the 30-minute recheck.
     */
    @Test
    public void newCandidateReopensProbingAfterPrivateVerdict() throws Exception {
        int portA = TestPorts.getPort();
        int portB = TestPorts.getPort();

        NonceRegistry noncesA = new NonceRegistry();
        AutonatV2DialBack.Binding dialBackA = new AutonatV2DialBack.Binding(noncesA);
        AutonatV2.Binding v2A = new AutonatV2.Binding(dialBackA);
        HostBuilder builderA = new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + portA)))
                .addProtocols(List.of(new Ping(), v2A, dialBackA));
        Host clientHost = builderA.build();

        List<ProtocolBinding> serverProtocols = new ArrayList<>(List.of(new Ping()));
        serverProtocols.addAll(AutonatV2.protocols());
        Host serverHost = new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + portB)))
                .addProtocols(serverProtocols)
                .build();

        clientHost.start().join();
        serverHost.start().join();
        try {
            Connection conn = clientHost.getNetwork()
                    .connect(serverHost.getPeerId(), new Multiaddr("/ip4/127.0.0.1/tcp/" + portB))
                    .get(15, TimeUnit.SECONDS);

            // Start with a candidate nothing listens on, so the dial-back fails and we latch PRIVATE.
            Multiaddr deadAddr = new Multiaddr("/ip4/127.0.0.1/tcp/" + TestPorts.getPort());
            Multiaddr liveAddr = new Multiaddr("/ip4/127.0.0.1/tcp/" + portA);
            List<Multiaddr> candidate = new CopyOnWriteArrayList<>(List.of(deadAddr));

            ReachabilityManager reachability = builderA.getReachability();
            AutoNatV2Client client = new AutoNatV2Client(clientHost, reachability, v2A, noncesA,
                    () -> candidate, 1);
            client.onConnection(conn);

            for (int i = 0; i < 100 && reachability.getReachability() != Reachability.PRIVATE; i++)
                Thread.sleep(100);
            Assert.assertEquals("unreachable candidate -> PRIVATE",
                    Reachability.PRIVATE, reachability.getReachability());

            // A reachable candidate now appears; the re-probe (normally driven by the poll thread) must
            // re-open probing and flip us to PUBLIC.
            candidate.set(0, liveAddr);
            client.probePending();

            for (int i = 0; i < 100 && reachability.getReachability() != Reachability.PUBLIC; i++)
                Thread.sleep(100);
            Assert.assertEquals("new reachable candidate re-opens probing and flips to PUBLIC",
                    Reachability.PUBLIC, reachability.getReachability());
            Assert.assertTrue("the newly reachable address is recorded as public",
                    reachability.getConfirmedPublicAddresses().contains(liveAddr));
        } finally {
            clientHost.stop();
            serverHost.stop();
        }
    }

    /**
     * The server dials only the first address it is offered, so without per-address rotation a second
     * address (a different transport) would never be verified. Offering two dialable addresses must result
     * in BOTH being confirmed, because the client rotates which one occupies the dialed slot.
     */
    @Test
    public void eachAddressIsVerifiedIndependently() throws Exception {
        int portA1 = TestPorts.getPort();
        int portA2 = TestPorts.getPort();
        int portB = TestPorts.getPort();

        NonceRegistry noncesA = new NonceRegistry();
        AutonatV2DialBack.Binding dialBackA = new AutonatV2DialBack.Binding(noncesA);
        AutonatV2.Binding v2A = new AutonatV2.Binding(dialBackA);
        HostBuilder builderA = new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + portA1),
                        new MultiAddress("/ip4/127.0.0.1/tcp/" + portA2)))
                .addProtocols(List.of(new Ping(), v2A, dialBackA));
        Host clientHost = builderA.build();

        List<ProtocolBinding> serverProtocols = new ArrayList<>(List.of(new Ping()));
        serverProtocols.addAll(AutonatV2.protocols());
        Host serverHost = new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + portB)))
                .addProtocols(serverProtocols)
                .build();

        clientHost.start().join();
        serverHost.start().join();
        try {
            Connection conn = clientHost.getNetwork()
                    .connect(serverHost.getPeerId(), new Multiaddr("/ip4/127.0.0.1/tcp/" + portB))
                    .get(15, TimeUnit.SECONDS);

            Multiaddr addr1 = new Multiaddr("/ip4/127.0.0.1/tcp/" + portA1);
            Multiaddr addr2 = new Multiaddr("/ip4/127.0.0.1/tcp/" + portA2);
            ReachabilityManager reachability = builderA.getReachability();
            AutoNatV2Client client = new AutoNatV2Client(clientHost, reachability, v2A, noncesA,
                    () -> List.of(addr1, addr2), 1);

            // Drive successive probes; each rotates the dialed slot to the still-undecided address.
            for (int i = 0; i < 100 && reachability.getConfirmedPublicAddresses().size() < 2; i++) {
                client.probePending();
                Thread.sleep(100);
            }
            List<Multiaddr> confirmed = reachability.getConfirmedPublicAddresses();
            Assert.assertEquals(Reachability.PUBLIC, reachability.getReachability());
            Assert.assertTrue("first address verified", confirmed.contains(addr1));
            Assert.assertTrue("second address verified too - it took its turn in the dialed slot",
                    confirmed.contains(addr2));
        } finally {
            clientHost.stop();
            serverHost.stop();
        }
    }
}
