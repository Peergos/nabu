package org.peergos.protocol.autonat;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;
import org.peergos.protocol.autonat.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/**
 * Drives our reachability using AutoNAT v2, testing each candidate address independently.
 *
 * The AutoNAT server dials only one address per request - "the first address it is willing to dial" - so
 * to verify a specific transport we must offer that address first. We therefore rotate which candidate
 * occupies the dialed (index 0) slot across successive probes, and tally results per address: an address
 * is reachable/unreachable once {@code votesRequired} distinct peers agree. This is how go-libp2p handles
 * it too (its addrs-reachability tracker rotates the primary address and keeps a per-address confidence),
 * and it means TCP and QUIC - which are distinct addresses - each get their own dial-backs rather than
 * QUIC always winning the single dialed slot.
 *
 * The node-level verdict is derived from the per-address results: PUBLIC (announcing exactly the addresses
 * that verified) as soon as any address is reachable, PRIVATE once every candidate has resolved unreachable.
 */
public class AutoNatV2Client {

    private static final int VOTES_REQUIRED = 3;
    private static final long RECHECK_INTERVAL_MINUTES = 30;
    // How often the background thread re-tests any still-undecided candidate against existing connections,
    // so late-appearing addresses (a UPnP mapping, a freshly-confirmed QUIC address) don't wait out the
    // full recheck interval.
    private static final long POLL_INTERVAL_SECONDS = 20;

    private enum AddrStatus { REACHABLE, UNREACHABLE, UNKNOWN }

    private final Host us;
    private final ReachabilityManager reachability;
    private final AutonatV2.Binding autonat;
    private final NonceRegistry nonces;
    private final Supplier<List<Multiaddr>> candidateAddresses;
    private final int votesRequired;

    // Per-address dial-back results: address -> (distinct peer -> reachable). A peer votes at most once per
    // address, so a verdict reflects agreement across distinct peers.
    private final Map<Multiaddr, Map<Multihash, Boolean>> addrVotes = new ConcurrentHashMap<>();
    private final Set<Multihash> inFlight = ConcurrentHashMap.newKeySet();
    private volatile Thread recheckThread;

    public AutoNatV2Client(Host us,
                           ReachabilityManager reachability,
                           AutonatV2.Binding autonat,
                           NonceRegistry nonces,
                           Supplier<List<Multiaddr>> candidateAddresses) {
        this(us, reachability, autonat, nonces, candidateAddresses, VOTES_REQUIRED);
    }

    public AutoNatV2Client(Host us,
                           ReachabilityManager reachability,
                           AutonatV2.Binding autonat,
                           NonceRegistry nonces,
                           Supplier<List<Multiaddr>> candidateAddresses,
                           int votesRequired) {
        this.us = us;
        this.reachability = reachability;
        this.autonat = autonat;
        this.nonces = nonces;
        this.candidateAddresses = candidateAddresses;
        this.votesRequired = votesRequired;
    }

    public void onConnection(Connection conn) {
        if (! conn.isInitiator())
            return;
        if (! hasUndecidedCandidate())
            return; // every candidate has resolved; the recheck thread handles periodic refresh
        probe(conn);
    }

    private void probe(Connection conn) {
        Multihash peer = Multihash.deserialize(conn.secureSession().getRemoteId().getBytes());
        if (! inFlight.add(peer))
            return;
        List<Multiaddr> candidates = candidateAddresses.get();
        Multiaddr target = pickTarget(candidates, peer);
        if (target == null) {
            inFlight.remove(peer);
            return; // nothing this peer can still contribute a vote towards
        }
        // Offer the target first so the server dials it; the rest ride along as fallbacks for the server.
        List<Multiaddr> ordered = new ArrayList<>(candidates.size());
        ordered.add(target);
        for (Multiaddr a : candidates)
            if (! a.equals(target))
                ordered.add(a);
        long nonce = ThreadLocalRandom.current().nextLong();
        CompletableFuture<Boolean> dialBack = nonces.expect(nonce);
        conn.muxerSession().createStream(autonat).getController()
                .thenCompose(controller -> controller.requestDial(ordered, nonce))
                .whenComplete((resp, err) -> {
                    inFlight.remove(peer);
                    nonces.forget(nonce);
                    if (err != null || resp == null)
                        return; // peer doesn't speak autonat v2, or the request failed
                    if (resp.getStatus() != Autonatv2.DialResponse.ResponseStatus.OK)
                        return; // refused / rejected - says nothing about reachability
                    int idx = resp.getAddrIdx();
                    if (idx < 0 || idx >= ordered.size())
                        return; // the server must tell us which address it actually dialed
                    Multiaddr dialed = ordered.get(idx);
                    if (resp.getDialStatus() == Autonatv2.DialStatus.OK && dialBack.getNow(false))
                        recordVote(dialed, peer, true);
                    else if (resp.getDialStatus() == Autonatv2.DialStatus.E_DIAL_ERROR)
                        recordVote(dialed, peer, false);
                    // E_DIAL_BACK_ERROR / UNUSED are inconclusive
                });
    }

    /** The still-undecided candidate this peer hasn't voted on yet, preferring the least-tested one. */
    private Multiaddr pickTarget(List<Multiaddr> candidates, Multihash peer) {
        Multiaddr best = null;
        int fewestVotes = Integer.MAX_VALUE;
        for (Multiaddr a : candidates) {
            if (statusOf(a) != AddrStatus.UNKNOWN)
                continue;
            Map<Multihash, Boolean> peerVotes = addrVotes.get(a);
            if (peerVotes != null && peerVotes.containsKey(peer))
                continue;
            int count = peerVotes == null ? 0 : peerVotes.size();
            if (count < fewestVotes) {
                fewestVotes = count;
                best = a;
            }
        }
        return best;
    }

    private void recordVote(Multiaddr addr, Multihash peer, boolean reachable) {
        addrVotes.computeIfAbsent(addr, a -> new ConcurrentHashMap<>()).put(peer, reachable);
        reevaluate();
    }

    private AddrStatus statusOf(Multiaddr addr) {
        Map<Multihash, Boolean> peerVotes = addrVotes.get(addr);
        if (peerVotes == null)
            return AddrStatus.UNKNOWN;
        long reachable = peerVotes.values().stream().filter(b -> b).count();
        if (reachable >= votesRequired)
            return AddrStatus.REACHABLE;
        if (peerVotes.size() - reachable >= votesRequired)
            return AddrStatus.UNREACHABLE;
        return AddrStatus.UNKNOWN;
    }

    /** Derive the node verdict from the per-address results and announce the addresses that verified. */
    private void reevaluate() {
        List<Multiaddr> current = candidateAddresses.get();
        List<Multiaddr> reachable = new ArrayList<>();
        boolean anyUnknown = false;
        for (Multiaddr a : current) {
            switch (statusOf(a)) {
                case REACHABLE: reachable.add(a); break;
                case UNKNOWN: anyUnknown = true; break;
                default: break;
            }
        }
        if (! reachable.isEmpty())
            reachability.setReachability(Reachability.PUBLIC, reachable);
        else if (! current.isEmpty() && ! anyUnknown)
            reachability.setReachability(Reachability.PRIVATE, List.of());
        // otherwise still converging - leave the current verdict untouched
    }

    private boolean hasUndecidedCandidate() {
        for (Multiaddr a : candidateAddresses.get())
            if (statusOf(a) == AddrStatus.UNKNOWN)
                return true;
        return false;
    }

    public synchronized void start() {
        if (recheckThread != null)
            return;
        recheckThread = new Thread(() -> {
            long lastFullRecheck = System.currentTimeMillis();
            while (! Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(POLL_INTERVAL_SECONDS));
                } catch (InterruptedException e) {
                    return;
                }
                if (System.currentTimeMillis() - lastFullRecheck >= TimeUnit.MINUTES.toMillis(RECHECK_INTERVAL_MINUTES)) {
                    recheck();
                    lastFullRecheck = System.currentTimeMillis();
                } else {
                    probePending();
                }
            }
        }, "autonat-v2-client");
        recheckThread.setDaemon(true);
        recheckThread.start();
    }

    public synchronized void stop() {
        if (recheckThread != null) {
            recheckThread.interrupt();
            recheckThread = null;
        }
    }

    /** Full re-test: discard all results and re-probe from scratch (periodic refresh). */
    public void recheck() {
        addrVotes.clear();
        for (Connection conn : us.getNetwork().getConnections())
            if (conn.isInitiator())
                probe(conn);
    }

    /**
     * Re-test any still-undecided candidate against existing connections. This picks up addresses that
     * appeared after an earlier verdict - a late UPnP mapping, or a QUIC/TCP address that only just reached
     * enough observations - without waiting for the 30-minute full recheck. Per-address state is preserved,
     * so already-decided addresses aren't re-tested and settled peers aren't re-probed.
     */
    public void probePending() {
        if (! hasUndecidedCandidate())
            return;
        for (Connection conn : us.getNetwork().getConnections())
            if (conn.isInitiator())
                probe(conn);
    }
}
