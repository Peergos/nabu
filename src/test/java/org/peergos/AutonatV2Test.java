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
}
