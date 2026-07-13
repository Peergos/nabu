package org.peergos.protocol.autonat;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.multiformats.Multiaddr;
import org.peergos.PeerAddresses;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
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

    public enum Reachability { UNKNOWN, PUBLIC, PRIVATE }

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
     * Register a self-discovered candidate public address (e.g. a UPnP/NAT-PMP mapped address) so the
     * AutoNAT client will ask peers to verify it. Only public addresses are kept.
     */
    public synchronized void addLocalCandidate(Multiaddr addr) {
        if (addr != null && PeerAddresses.isPublic(addr, false))
            localCandidates.add(addr);
    }

    public Reachability getReachability() {
        return reachability;
    }

    public synchronized List<Multiaddr> getConfirmedPublicAddresses() {
        return new ArrayList<>(confirmedPublic);
    }

    /**
     * Update the reachability verdict and the confirmed public addresses. Called by the AutoNAT client
     * once dial-backs resolve. Listeners fire only when the verdict actually changes.
     */
    public void setReachability(Reachability updated, Collection<Multiaddr> publicAddrs) {
        Reachability previous;
        synchronized (this) {
            previous = this.reachability;
            this.reachability = updated;
            this.confirmedPublic.clear();
            if (publicAddrs != null)
                this.confirmedPublic.addAll(publicAddrs);
        }
        if (previous != updated)
            for (Consumer<Reachability> listener : listeners)
                listener.accept(updated);
    }

    /** Register a callback fired whenever the reachability verdict transitions. */
    public void addListener(Consumer<Reachability> listener) {
        listeners.add(listener);
    }
}
