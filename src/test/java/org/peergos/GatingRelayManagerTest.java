package org.peergos;

import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.protocol.circuit.*;

import java.util.concurrent.atomic.*;

public class GatingRelayManagerTest {

    @Test
    public void refusesReservationsUntilPubliclyReachable() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        PeerId us = PeerId.fromPubKey(priv.publicKey());
        AtomicBoolean reachable = new AtomicBoolean(false);
        GatingRelayManager manager = new GatingRelayManager(priv, us, 5, 1 << 20, 120, reachable::get);
        PeerId requestor = PeerId.random();
        Multiaddr observed = new Multiaddr("/ip4/1.2.3.4/tcp/4001");

        Assert.assertTrue("no reservation while not reachable",
                manager.createReservation(requestor, observed).isEmpty());

        reachable.set(true);
        Assert.assertTrue("reservation granted once publicly reachable",
                manager.createReservation(requestor, observed).isPresent());
    }

    @Test
    public void honoursConcurrentLimit() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        PeerId us = PeerId.fromPubKey(priv.publicKey());
        GatingRelayManager manager = new GatingRelayManager(priv, us, 2, 1 << 20, 120, () -> true);
        Multiaddr observed = new Multiaddr("/ip4/1.2.3.4/tcp/4001");

        Assert.assertTrue(manager.createReservation(PeerId.random(), observed).isPresent());
        Assert.assertTrue(manager.createReservation(PeerId.random(), observed).isPresent());
        Assert.assertTrue("third reservation exceeds the limit",
                manager.createReservation(PeerId.random(), observed).isEmpty());
    }
}
