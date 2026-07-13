package org.peergos.protocol.circuit;

import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.protocol.circuit.CircuitHopProtocol;
import org.peergos.protocol.autonat.ReachabilityManager;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.function.*;

/**
 * A relay {@link CircuitHopProtocol.RelayManager} that only grants reservations while we are ourselves
 * publicly reachable, so a NATed node never advertises itself as a usable relay. Delegates the actual
 * reservation bookkeeping (vouchers, concurrency limit) to the library's default manager.
 */
public class GatingRelayManager implements CircuitHopProtocol.RelayManager {

    private final CircuitHopProtocol.RelayManager delegate;
    private final Supplier<Boolean> shouldRelay;

    public GatingRelayManager(CircuitHopProtocol.RelayManager delegate, Supplier<Boolean> shouldRelay) {
        this.delegate = delegate;
        this.shouldRelay = shouldRelay;
    }

    public static GatingRelayManager reachabilityGated(PrivKey priv, PeerId us, int concurrent, ReachabilityManager reachability) {
        return new GatingRelayManager(CircuitHopProtocol.RelayManager.limitTo(priv, us, concurrent),
                () -> reachability.getReachability() == Reachability.PUBLIC);
    }

    @Override
    public boolean hasReservation(PeerId source) {
        return delegate.hasReservation(source);
    }

    @Override
    public Optional<CircuitHopProtocol.Reservation> createReservation(PeerId requestor, Multiaddr observedAddr) {
        if (! shouldRelay.get())
            return Optional.empty();
        return delegate.createReservation(requestor, observedAddr);
    }

    @Override
    public Optional<CircuitHopProtocol.Reservation> allowConnection(PeerId target, PeerId initiator) {
        return delegate.allowConnection(target, initiator);
    }
}
