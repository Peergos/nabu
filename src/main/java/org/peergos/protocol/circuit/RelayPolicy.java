package org.peergos.protocol.circuit;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.circuit.RelayTransport.CandidateRelay;

import java.util.*;
import java.util.function.*;

/** Lets an application decide which relays this peer reserves on, who may reserve on it, and who may connect
 *  through it, instead of any connected or DHT peer.
 */
public class RelayPolicy {
    public final Function<Host, List<CandidateRelay>> candidates;
    /** The requestor and the address we observe it on. */
    public final BiPredicate<PeerId, Multiaddr> mayReserve;
    /** The reserved target and the initiator dialling it. */
    public final BiPredicate<PeerId, PeerId> mayConnect;
    public final int maxReservations;

    public RelayPolicy(Function<Host, List<CandidateRelay>> candidates,
                       BiPredicate<PeerId, Multiaddr> mayReserve,
                       BiPredicate<PeerId, PeerId> mayConnect,
                       int maxReservations) {
        this.candidates = candidates;
        this.mayReserve = mayReserve;
        this.mayConnect = mayConnect;
        this.maxReservations = maxReservations;
    }
}
