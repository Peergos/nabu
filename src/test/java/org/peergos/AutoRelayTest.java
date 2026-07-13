package org.peergos;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.protocol.circuit.RelayTransport.CandidateRelay;
import org.junit.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;
import org.peergos.protocol.circuit.*;

import java.util.*;
import java.util.concurrent.*;

public class AutoRelayTest {

    private static CandidateRelay candidate() {
        return new CandidateRelay(PeerId.random(), List.of(new Multiaddr("/ip4/1.2.3.4/tcp/4001")));
    }

    @Test
    public void reservesUpToTargetOnlyWhenPrivate() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ReachabilityManager reachability = new ReachabilityManager();
            List<CandidateRelay> relays = List.of(candidate(), candidate(), candidate());
            List<PeerId> reserved = new CopyOnWriteArrayList<>();

            AutoRelay autoRelay = new AutoRelay(reachability, () -> relays,
                    c -> { reserved.add(c.id); return CompletableFuture.completedFuture(true); },
                    2, 45, scheduler);
            autoRelay.start();

            // UNKNOWN: nothing reserved
            autoRelay.maintain();
            Assert.assertTrue(reserved.isEmpty());

            // PRIVATE: reserve up to the target of 2
            reachability.setReachability(Reachability.PRIVATE, List.of());
            Assert.assertEquals(2, autoRelay.activeRelays().size());
            Assert.assertEquals(2, reserved.size());

            // PUBLIC: reservations are released
            reachability.setReachability(Reachability.PUBLIC, List.of());
            Assert.assertTrue(autoRelay.activeRelays().isEmpty());
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    public void skipsRelaysThatRefuse() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            ReachabilityManager reachability = new ReachabilityManager();
            CandidateRelay bad = candidate();
            CandidateRelay good1 = candidate();
            CandidateRelay good2 = candidate();
            List<CandidateRelay> relays = List.of(bad, good1, good2);

            AutoRelay autoRelay = new AutoRelay(reachability, () -> relays,
                    c -> CompletableFuture.completedFuture(! c.id.equals(bad.id)),
                    2, 45, scheduler);
            autoRelay.start();

            reachability.setReachability(Reachability.PRIVATE, List.of());
            Set<PeerId> active = autoRelay.activeRelays();
            Assert.assertEquals(2, active.size());
            Assert.assertFalse("refusing relay is not held", active.contains(bad.id));
            Assert.assertTrue(active.contains(good1.id) && active.contains(good2.id));
        } finally {
            scheduler.shutdownNow();
        }
    }
}
