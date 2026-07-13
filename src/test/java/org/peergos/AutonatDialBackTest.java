package org.peergos;

import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;
import org.peergos.protocol.autonat.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Exercises the AutoNAT dial-back primitive end-to-end: it must force a fresh connection and confirm
 * the peer identity, returning false for unreachable or mismatched targets.
 */
public class AutonatDialBackTest {

    private static Host loopbackHost(int port) {
        return new HostBuilder(new RamAddressBook())
                .generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + port)))
                .addProtocols(List.of(new Ping(), new AutonatProtocol.Binding()))
                .build();
    }

    @Test
    public void dialBackVerifiesReachabilityAndIdentity() throws Exception {
        int serverPort = TestPorts.getPort();
        int targetPort = TestPorts.getPort();
        Host server = loopbackHost(serverPort);
        Host target = loopbackHost(targetPort);
        server.start().join();
        target.start().join();
        try {
            Multiaddr targetAddr = new Multiaddr("/ip4/127.0.0.1/tcp/" + targetPort);

            // A live, correctly-identified target is reachable
            Boolean reachable = AutonatProtocol.dialBack(server, target.getPeerId(), targetAddr)
                    .get(20, TimeUnit.SECONDS);
            Assert.assertTrue("live target should be reachable", reachable);

            // Nobody listening -> not reachable
            Multiaddr dead = new Multiaddr("/ip4/127.0.0.1/tcp/1");
            Boolean deadReachable = AutonatProtocol.dialBack(server, target.getPeerId(), dead)
                    .get(20, TimeUnit.SECONDS);
            Assert.assertFalse("dead address should not be reachable", deadReachable);

            // Right address, wrong expected peer id -> rejected
            Boolean mismatch = AutonatProtocol.dialBack(server, server.getPeerId(), targetAddr)
                    .get(20, TimeUnit.SECONDS);
            Assert.assertFalse("peer id mismatch should be rejected", mismatch);
        } finally {
            server.stop();
            target.stop();
        }
    }
}
