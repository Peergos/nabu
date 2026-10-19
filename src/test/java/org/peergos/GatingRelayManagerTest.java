package org.peergos;

import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.protocol.circuit.*;

import java.time.*;
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

    @Test
    public void expiredReservationsFreeTheirSlots() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        PeerId us = PeerId.fromPubKey(priv.publicKey());
        AtomicReference<LocalDateTime> now = new AtomicReference<>(LocalDateTime.of(2026, 1, 1, 0, 0));
        GatingRelayManager manager = new GatingRelayManager(priv, us, 1, 1 << 20, 120, () -> true,
                (p, a) -> true, (t, i) -> true, now::get);
        Multiaddr observed = new Multiaddr("/ip4/1.2.3.4/tcp/4001");
        PeerId first = PeerId.random();
        Assert.assertTrue(manager.createReservation(first, observed).isPresent());
        Assert.assertTrue(manager.createReservation(PeerId.random(), observed).isEmpty());
        now.set(now.get().plusHours(2));
        Assert.assertFalse(manager.hasReservation(first));
        Assert.assertTrue("the expired one no longer holds the only slot",
                manager.createReservation(PeerId.random(), observed).isPresent());
    }

    @Test
    public void thePolicyDecidesWhoReservesAndWhoConnects() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        PeerId us = PeerId.fromPubKey(priv.publicKey());
        PeerId known = PeerId.random(), stranger = PeerId.random(), friend = PeerId.random();
        GatingRelayManager manager = new GatingRelayManager(priv, us, 5, 1 << 20, 120, () -> true,
                (p, a) -> p.equals(known), (t, i) -> i.equals(friend), LocalDateTime::now);
        Multiaddr observed = new Multiaddr("/ip4/1.2.3.4/tcp/4001");
        Assert.assertTrue(manager.createReservation(stranger, observed).isEmpty());
        Assert.assertTrue(manager.createReservation(known, observed).isPresent());
        Assert.assertTrue(manager.allowConnection(known, friend).isPresent());
        Assert.assertTrue(manager.allowConnection(known, stranger).isEmpty());
        Assert.assertTrue("no reservation, no connection", manager.allowConnection(stranger, friend).isEmpty());
    }

}
