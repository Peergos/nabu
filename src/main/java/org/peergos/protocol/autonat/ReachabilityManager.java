package org.peergos.protocol.autonat;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.multiformats.Multiaddr;
import org.peergos.PeerAddresses;
import org.peergos.util.Logging;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;

/**
 * Tracks whether this node is publicly reachable, and the set of external addresses to announce.
 *
 * This is the switch that turns AutoRelay, DCUtR and UPnP announcement on and off. It accumulates
 * candidate external addresses reported by remote peers (via the identify observedAddr field), and
 * holds the reachability verdict that the AutoNAT client sets once dial-backs resolve. Until AutoNAT
 * is wired in, reachability stays {@link Reachability#UNKNOWN} and only candidate addresses accrue.
 */
public class ReachabilityManager {

    private static final Logger LOG = Logging.LOG();

    public enum Reachability { UNKNOWN, PUBLIC, PRIVATE }

    /**
     * The NAT's port-mapping behaviour, inferred from DCUtR hole-punch outcomes. A successful hole punch
     * means our socket is mapped consistently enough to be reachable directly ({@link #ENDPOINT_INDEPENDENT});
     * hole punches that keep failing to distinct peers indicate a {@link #SYMMETRIC} NAT (a different
     * external port per destination), which defeats hole punching and forces us to stay behind relays.
     *
     * (This can't be probed directly with a STUN-style test on jvm-libp2p: its QUIC {@code dial} binds a
     * fresh ephemeral port each time so the observed address is a throwaway mapping, and {@code
     * dialAsListener} only completes during a mutual hole punch - so hole-punch success/failure is the
     * signal we actually have.)
     */
    public enum NatType { UNKNOWN, ENDPOINT_INDEPENDENT, SYMMETRIC }

    // Distinct peers we failed to hole punch to; enough of them implies a symmetric NAT.
    private static final int HOLE_PUNCH_FAILURES_FOR_SYMMETRIC = 2;
    private final Set<Multihash> holePunchFailures = new HashSet<>();
    private volatile NatType natType = NatType.UNKNOWN;

    // Distinct remote peers that must observe an address before we treat it as a real candidate.
    private final int confirmationsRequired;
    // Candidate external addresses observed by remote peers, mapped to the distinct peers reporting each.
    private final Map<Multiaddr, Set<Multihash>> observations = new HashMap<>();
    // Addresses AutoNAT has confirmed are dialable from the public internet.
    private final Set<Multiaddr> confirmedPublic = new LinkedHashSet<>();
    // Self-discovered candidate addresses (e.g. from UPnP/NAT-PMP port mapping) to be verified by AutoNAT.
    private final Set<Multiaddr> localCandidates = new LinkedHashSet<>();
    private final List<Consumer<Reachability>> listeners = new CopyOnWriteArrayList<>();
    private volatile Reachability reachability = Reachability.UNKNOWN;

    // Snapshot of what was last written to the NAT-status log, so we log only on a real change. Guarded by this.
    private Reachability lastLoggedReachability = Reachability.UNKNOWN;
    private NatType lastLoggedNatType = NatType.UNKNOWN;
    private Set<Multiaddr> lastLoggedExternal = new LinkedHashSet<>();

    public ReachabilityManager() {
        this(3);
    }

    public ReachabilityManager(int confirmationsRequired) {
        this.confirmationsRequired = confirmationsRequired;
    }

    /** Record a public address that a remote peer observed us connecting from (identify observedAddr). */
    public synchronized void observeAddress(Multiaddr observed, Multihash reporter) {
        if (observed == null || reporter == null)
            return;
        if (! PeerAddresses.isPublic(observed, false))
            return;
        observations.computeIfAbsent(observed, a -> new HashSet<>()).add(reporter);
        logStatusIfChanged();
    }

    /** External addresses reported by at least {@code confirmationsRequired} distinct peers. */
    public synchronized List<Multiaddr> getCandidateAddresses() {
        return observations.entrySet().stream()
                .filter(e -> e.getValue().size() >= confirmationsRequired)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /** All candidate public addresses to test: peer-observed plus self-discovered (e.g. UPnP). */
    public synchronized List<Multiaddr> getAllObservedAddresses() {
        Set<Multiaddr> all = new LinkedHashSet<>(observations.keySet());
        all.addAll(localCandidates);
        return new ArrayList<>(all);
    }

    /**
     * External IPs that at least {@code minReporters} distinct peers observed us at. Since jvm-libp2p
     * dials from ephemeral ports, the observed *port* is unreliable, but the *IP* is stable, so callers
     * pair these with our listen port to predict our real external address for AutoNAT to verify.
     */
    public synchronized Set<String> getObservedHosts(int minReporters) {
        Map<String, Set<Multihash>> reportersByHost = new HashMap<>();
        for (Map.Entry<Multiaddr, Set<Multihash>> e : observations.entrySet()) {
            String host = hostOf(e.getKey());
            if (host != null)
                reportersByHost.computeIfAbsent(host, k -> new HashSet<>()).addAll(e.getValue());
        }
        return reportersByHost.entrySet().stream()
                .filter(e -> e.getValue().size() >= minReporters)
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Self-discovered candidates (e.g. UPnP-mapped addresses), which have real ports unlike observations. */
    public synchronized List<Multiaddr> getLocalCandidates() {
        return new ArrayList<>(localCandidates);
    }

    private static String hostOf(Multiaddr addr) {
        try {
            return new io.ipfs.multiaddr.MultiAddress(addr.toString()).getHost();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Register a self-discovered candidate public address (e.g. a UPnP/NAT-PMP mapped address) so the
     * AutoNAT client will ask peers to verify it. Only public addresses are kept.
     */
    public synchronized void addLocalCandidate(Multiaddr addr) {
        if (addr != null && PeerAddresses.isPublic(addr, false)) {
            localCandidates.add(addr);
            logStatusIfChanged();
        }
    }

    public Reachability getReachability() {
        return reachability;
    }

    public synchronized List<Multiaddr> getConfirmedPublicAddresses() {
        return new ArrayList<>(confirmedPublic);
    }

    /**
     * Update the reachability verdict and the confirmed public addresses. Called by the AutoNAT client
     * once dial-backs resolve. Listeners fire only when the verdict actually changes; the NAT status is
     * logged whenever the verdict, NAT type, or the set of known external addresses changes.
     */
    public void setReachability(Reachability updated, Collection<Multiaddr> publicAddrs) {
        Set<Multiaddr> updatedAddrs = publicAddrs == null ? Set.of() : new LinkedHashSet<>(publicAddrs);
        Reachability previous;
        synchronized (this) {
            previous = this.reachability;
            this.reachability = updated;
            this.confirmedPublic.clear();
            this.confirmedPublic.addAll(updatedAddrs);
            logStatusIfChanged();
        }
        if (previous != updated)
            for (Consumer<Reachability> listener : listeners)
                listener.accept(updated);
    }

    /** The external addresses we currently know: AutoNAT-confirmed, peer-observed candidates, and UPnP-mapped. */
    private Set<Multiaddr> knownExternalAddresses() {
        Set<Multiaddr> ext = new LinkedHashSet<>(confirmedPublic);
        for (Map.Entry<Multiaddr, Set<Multihash>> e : observations.entrySet())
            if (e.getValue().size() >= confirmationsRequired)
                ext.add(e.getKey());
        ext.addAll(localCandidates);
        return ext;
    }

    /**
     * Log the NAT traversal status whenever the verdict, the inferred NAT type, or the set of known
     * external addresses changes. Called while holding this lock from every place those change, so a newly
     * discovered external address is logged even while we are still UNKNOWN/PRIVATE (i.e. before AutoNAT
     * has confirmed it dialable).
     */
    private void logStatusIfChanged() {
        Set<Multiaddr> ext = knownExternalAddresses();
        if (reachability == lastLoggedReachability && natType == lastLoggedNatType && ext.equals(lastLoggedExternal))
            return;
        lastLoggedReachability = reachability;
        lastLoggedNatType = natType;
        lastLoggedExternal = ext;
        String addrs = ext.isEmpty()
                ? "none discovered yet"
                : ext.stream().map(Multiaddr::toString).collect(Collectors.joining(", "));
        String qualifier = ext.isEmpty() ? ""
                : confirmedPublic.isEmpty() ? " (observed, not yet confirmed reachable)"
                : " (confirmed reachable)";
        LOG.info("NAT traversal status: reachability=" + reachability
                + ", NAT type=" + natType
                + ", external addresses=[" + addrs + "]" + qualifier);
    }

    /** Register a callback fired whenever the reachability verdict transitions. */
    public void addListener(Consumer<Reachability> listener) {
        listeners.add(listener);
    }

    public NatType getNatType() {
        return natType;
    }

    private void setNatType(NatType updated) {
        NatType previous = this.natType;
        this.natType = updated;
        if (previous != updated && updated != NatType.UNKNOWN) {
            if (updated == NatType.SYMMETRIC)
                LOG.info("NAT type inferred: SYMMETRIC - DCUtR hole punching keeps failing to distinct "
                        + "peers, so direct connections aren't possible; this node must stay reachable via "
                        + "circuit relays");
            else
                LOG.info("NAT type inferred: ENDPOINT_INDEPENDENT - a DCUtR hole punch succeeded, so direct "
                        + "connections are possible");
            logStatusIfChanged();
        }
    }

    /**
     * Feed the outcome of a DCUtR hole-punch attempt. A success means our external mapping is consistent
     * enough for a direct connection (endpoint-independent); repeated failures to distinct peers indicate
     * a symmetric NAT. This is the reachability-type signal we actually have, since jvm-libp2p cannot make
     * an ordinary dial from the listen socket to run a STUN-style probe.
     */
    public synchronized void recordHolePunchOutcome(Multihash peer, boolean directConnectionEstablished) {
        if (directConnectionEstablished) {
            holePunchFailures.clear();
            setNatType(NatType.ENDPOINT_INDEPENDENT);
        } else {
            holePunchFailures.add(peer);
            if (holePunchFailures.size() >= HOLE_PUNCH_FAILURES_FOR_SYMMETRIC)
                setNatType(NatType.SYMMETRIC);
        }
    }
}
