package org.peergos;

import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.*;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.protocol.circuit.*;

import java.time.*;
import java.util.*;
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


    @Test
    public void bandwidthIsMeteredPerTargetAcrossCircuitsAndInTotal() {
        long[] nanos = {0};
        RelayBandwidth bw = new RelayBandwidth(1_000, 10_000, 3_000, 30_000, () -> nanos[0]);
        PeerId a = PeerId.random(), b = PeerId.random();
        Assert.assertEquals("within the burst", 0, bw.charge(a, 6_000));
        Assert.assertEquals("another circuit to the same target shares its budget", 0, bw.charge(a, 4_000));
        Assert.assertEquals("a second of debt at 1 KB/s", 1_000, bw.charge(a, 1_000));
        Assert.assertEquals("another target has its own", 0, bw.charge(b, 10_000));
        nanos[0] += 2_000_000_000L;
        Assert.assertEquals("paid off, and a second's worth refilled", 0, bw.charge(a, 1_000));

        Assert.assertEquals(0, bw.charge(PeerId.random(), 10_000));
        Assert.assertTrue("a fresh target, but the relay's total has run out", bw.charge(PeerId.random(), 10_000) > 0);
    }

    @Test
    public void aMeteredRelayLiftsThePerCircuitLimit() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        PeerId us = PeerId.fromPubKey(priv.publicKey());
        RelayBandwidth bw = new RelayBandwidth(1_000, 1_000, 1_000_000, 1_000_000);
        GatingRelayManager manager = new GatingRelayManager(priv, us, 5, 0, 3600, () -> true,
                (p, a) -> Optional.of(Duration.ofHours(1)), (t, i) -> true, LocalDateTime::now, Optional.of(bw));
        PeerId target = PeerId.random();
        Assert.assertEquals(0, manager.createReservation(target, new Multiaddr("/ip4/1.2.3.4/tcp/4001")).get().maxBytes);
        Assert.assertEquals(0, manager.forwarded(target, 1_000));
        Assert.assertTrue(manager.forwarded(target, 1_000) > 0);
        Assert.assertEquals("unmetered", 0, new GatingRelayManager(priv, us, 5, 1 << 20, 120, () -> true).forwarded(target, 1 << 30));
    }


    @Test
    public void thePolicySaysHowLongEachReservationLasts() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        PeerId us = PeerId.fromPubKey(priv.publicKey());
        PeerId leased = PeerId.random(), full = PeerId.random();
        AtomicReference<LocalDateTime> now = new AtomicReference<>(LocalDateTime.of(2026, 1, 1, 0, 0));
        GatingRelayManager manager = new GatingRelayManager(priv, us, 5, 0, 3600, () -> true,
                (p, a) -> Optional.of(p.equals(leased) ? Duration.ofMinutes(15) : Duration.ofHours(1)),
                (t, i) -> true, now::get, Optional.empty());
        Multiaddr observed = new Multiaddr("/ip4/1.2.3.4/tcp/4001");
        Assert.assertEquals(now.get().plusMinutes(15), manager.createReservation(leased, observed).get().expiry);
        Assert.assertEquals(now.get().plusHours(1), manager.createReservation(full, observed).get().expiry);
        now.set(now.get().plusMinutes(20));
        Assert.assertFalse(manager.hasReservation(leased));
        Assert.assertTrue(manager.hasReservation(full));
    }

}
