package org.peergos.protocol.circuit;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.circuit.RelayTransport.CandidateRelay;
import org.peergos.protocol.autonat.ReachabilityManager;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Keeps us reachable via circuit relays while we are not publicly reachable.
 *
 * When {@link ReachabilityManager} reports {@link Reachability#PRIVATE} we reserve slots on a handful of
 * relays (discovered via the supplied candidate source) and renew them before they expire; when we
 * become {@link Reachability#PUBLIC} again we let them lapse. Reservations are made explicitly (rather
 * than via the library's automatic {@code setRelayCount} path, which has a renewal bug) by listening on
 * the relay's {@code /p2p-circuit} address.
 */
public class AutoRelay {

    private static final int DEFAULT_TARGET_RELAYS = 2;
    private static final long DEFAULT_RENEW_MINUTES = 45;
    private static final long RESERVE_TIMEOUT_SECONDS = 30;

    private final ReachabilityManager reachability;
    private final Supplier<List<CandidateRelay>> candidates;
    private final Function<CandidateRelay, CompletableFuture<Boolean>> reserve;
    private final int targetRelays;
    private final long renewMinutes;
    private final ScheduledExecutorService scheduler;
    private final Set<PeerId> active = ConcurrentHashMap.newKeySet();

    public AutoRelay(ReachabilityManager reachability,
                     Supplier<List<CandidateRelay>> candidates,
                     Function<CandidateRelay, CompletableFuture<Boolean>> reserve,
                     ScheduledExecutorService scheduler) {
        this(reachability, candidates, reserve, DEFAULT_TARGET_RELAYS, DEFAULT_RENEW_MINUTES, scheduler);
    }

    public AutoRelay(ReachabilityManager reachability,
                     Supplier<List<CandidateRelay>> candidates,
                     Function<CandidateRelay, CompletableFuture<Boolean>> reserve,
                     int targetRelays,
                     long renewMinutes,
                     ScheduledExecutorService scheduler) {
        this.reachability = reachability;
        this.candidates = candidates;
        this.reserve = reserve;
        this.targetRelays = targetRelays;
        this.renewMinutes = renewMinutes;
        this.scheduler = scheduler;
    }

    public void start() {
        reachability.addListener(this::onReachabilityChange);
        scheduler.scheduleAtFixedRate(this::maintain, renewMinutes, renewMinutes, TimeUnit.MINUTES);
    }

    public Set<PeerId> activeRelays() {
        return new HashSet<>(active);
    }

    private void onReachabilityChange(Reachability updated) {
        if (updated == Reachability.PRIVATE)
            maintain();
        else if (updated == Reachability.PUBLIC)
            active.clear(); // stop renewing; reservations lapse on their own
    }

    /** Renew existing relay reservations and top up to the target count. */
    public synchronized void maintain() {
        if (reachability.getReachability() != Reachability.PRIVATE)
            return;
        List<CandidateRelay> discovered = candidates.get();
        // renew the ones we already hold
        for (CandidateRelay candidate : discovered) {
            if (active.contains(candidate.id))
                tryReserve(candidate);
        }
        // top up to the target
        for (CandidateRelay candidate : discovered) {
            if (active.size() >= targetRelays)
                break;
            if (active.contains(candidate.id))
                continue;
            tryReserve(candidate);
        }
    }

    private void tryReserve(CandidateRelay candidate) {
        try {
            if (reserve.apply(candidate).get(RESERVE_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                active.add(candidate.id);
            else
                active.remove(candidate.id);
        } catch (Exception e) {
            active.remove(candidate.id);
        }
    }

    /**
     * Reserve a slot on a relay by listening on its {@code /p2p-circuit} address, and advertise the
     * resulting relayed address for ourselves so peers learn it via identify.
     */
    public static CompletableFuture<Boolean> reserveOnRelay(Host us, CandidateRelay candidate) {
        if (candidate.addrs.isEmpty())
            return CompletableFuture.completedFuture(false);
        Multiaddr relayAddr = candidate.addrs.get(0).withP2P(candidate.id);
        Multiaddr circuit = new Multiaddr(relayAddr + "/p2p-circuit");
        return us.getNetwork().listen(circuit)
                .thenApply(x -> {
                    Multiaddr dialAddr = relayAddr.concatenated(
                            new Multiaddr("/p2p-circuit/p2p/" + us.getPeerId().toBase58()));
                    us.getAddressBook().addAddrs(us.getPeerId(), 0, dialAddr);
                    return true;
                })
                .exceptionally(t -> false);
    }
}
