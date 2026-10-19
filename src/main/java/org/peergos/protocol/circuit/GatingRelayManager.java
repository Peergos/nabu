package org.peergos.protocol.circuit;

import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.protocol.circuit.CircuitHopProtocol;
import io.libp2p.protocol.circuit.CircuitHopProtocol.Reservation;
import org.peergos.protocol.autonat.ReachabilityManager;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.time.*;
import java.util.*;
import java.util.function.*;

/**
 * A relay {@link CircuitHopProtocol.RelayManager} that only grants reservations while we are ourselves
 * publicly reachable, so a NATed node never advertises itself as a usable relay.
 *
 * We issue reservations directly (rather than via the library's {@code limitTo}) so we can set a
 * realistic per-connection data limit: the library default of 4096 bytes is barely enough to establish
 * a relayed connection (multistream + noise + yamux) and leaves no room to run DCUtR over it.
 */
public class GatingRelayManager implements CircuitHopProtocol.RelayManager {

    // Enough headroom to establish a relayed connection and negotiate DCUtR over it.
    private static final long DEFAULT_MAX_BYTES = 1 << 20; // 1 MiB
    private static final int DEFAULT_DURATION_SECONDS = 120;

    private final PrivKey priv;
    private final PeerId relayId;
    private final int concurrent;
    private final long maxBytes;
    private final int durationSeconds;
    private final Supplier<Boolean> shouldRelay;
    private final BiPredicate<PeerId, Multiaddr> mayReserve;
    private final BiPredicate<PeerId, PeerId> mayConnect;
    private final Supplier<LocalDateTime> clock;
    private final Map<PeerId, Reservation> reservations = new HashMap<>();

    public GatingRelayManager(PrivKey priv, PeerId relayId, int concurrent, long maxBytes,
                              int durationSeconds, Supplier<Boolean> shouldRelay) {
        this(priv, relayId, concurrent, maxBytes, durationSeconds, shouldRelay, (p, a) -> true, (t, i) -> true, LocalDateTime::now);
    }

    public GatingRelayManager(PrivKey priv, PeerId relayId, int concurrent, long maxBytes,
                              int durationSeconds, Supplier<Boolean> shouldRelay,
                              BiPredicate<PeerId, Multiaddr> mayReserve, BiPredicate<PeerId, PeerId> mayConnect,
                              Supplier<LocalDateTime> clock) {
        this.priv = priv;
        this.relayId = relayId;
        this.concurrent = concurrent;
        this.maxBytes = maxBytes;
        this.durationSeconds = durationSeconds;
        this.shouldRelay = shouldRelay;
        this.mayReserve = mayReserve;
        this.mayConnect = mayConnect;
        this.clock = clock;
    }

    public static GatingRelayManager reachabilityGated(PrivKey priv, PeerId us, int concurrent, ReachabilityManager reachability) {
        return new GatingRelayManager(priv, us, concurrent, DEFAULT_MAX_BYTES, DEFAULT_DURATION_SECONDS,
                () -> reachability.getReachability() == Reachability.PUBLIC);
    }

    public static GatingRelayManager withPolicy(PrivKey priv, PeerId us, RelayPolicy policy, ReachabilityManager reachability) {
        return new GatingRelayManager(priv, us, policy.maxReservations, DEFAULT_MAX_BYTES, DEFAULT_DURATION_SECONDS,
                () -> reachability.getReachability() == Reachability.PUBLIC, policy.mayReserve, policy.mayConnect, LocalDateTime::now);
    }

    /** Without this a relay fills up with reservations that expired long ago and refuses every new one. */
    private void dropExpired() {
        LocalDateTime now = clock.get();
        reservations.values().removeIf(r -> r.expiry.isBefore(now));
    }

    @Override
    public synchronized boolean hasReservation(PeerId source) {
        dropExpired();
        return reservations.containsKey(source);
    }

    @Override
    public synchronized Optional<Reservation> createReservation(PeerId requestor, Multiaddr observedAddr) {
        if (! shouldRelay.get())
            return Optional.empty();
        dropExpired();
        if (! mayReserve.test(requestor, observedAddr))
            return Optional.empty();
        if (reservations.size() >= concurrent && ! reservations.containsKey(requestor))
            return Optional.empty();
        LocalDateTime now = clock.get();
        LocalDateTime expiry = now.plusHours(1);
        byte[] voucher = CircuitHopProtocol.createVoucher(priv, relayId, requestor, now);
        Reservation resv = new Reservation(expiry, durationSeconds, maxBytes, voucher, new Multiaddr[]{observedAddr});
        reservations.put(requestor, resv);
        return Optional.of(resv);
    }

    @Override
    public synchronized Optional<Reservation> allowConnection(PeerId target, PeerId initiator) {
        dropExpired();
        if (! reservations.containsKey(target) || ! mayConnect.test(target, initiator))
            return Optional.empty();
        return Optional.ofNullable(reservations.get(target));
    }
}
