package org.peergos;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.protocol.circuit.CircuitHopProtocol;
import io.libp2p.protocol.circuit.CircuitHopProtocol.Reservation;
import org.junit.*;
import org.peergos.protocol.circuit.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class GatingRelayManagerTest {

    /** A delegate that always grants, so the test isolates the reachability gate. */
    private static CircuitHopProtocol.RelayManager alwaysGrants() {
        Reservation resv = new Reservation(LocalDateTime.now().plusHours(1), 3600, 4096, new byte[0], new Multiaddr[0]);
        return new CircuitHopProtocol.RelayManager() {
            public boolean hasReservation(PeerId source) { return true; }
            public Optional<Reservation> createReservation(PeerId requestor, Multiaddr observed) { return Optional.of(resv); }
            public Optional<Reservation> allowConnection(PeerId target, PeerId initiator) { return Optional.of(resv); }
        };
    }

    @Test
    public void refusesReservationsUntilPubliclyReachable() {
        AtomicBoolean reachable = new AtomicBoolean(false);
        GatingRelayManager manager = new GatingRelayManager(alwaysGrants(), reachable::get);
        PeerId requestor = PeerId.random();
        Multiaddr observed = new Multiaddr("/ip4/1.2.3.4/tcp/4001");

        Assert.assertTrue("no reservation while not reachable",
                manager.createReservation(requestor, observed).isEmpty());

        reachable.set(true);
        Assert.assertTrue("reservation granted once publicly reachable",
                manager.createReservation(requestor, observed).isPresent());
    }
}
