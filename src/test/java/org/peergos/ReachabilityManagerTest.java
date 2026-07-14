package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.concurrent.atomic.*;

public class ReachabilityManagerTest {

    private static Multihash peer(int i) {
        byte[] raw = new byte[32];
        raw[0] = (byte) i;
        return new Multihash(Multihash.Type.sha2_256, raw);
    }

    @Test
    public void candidateNeedsMultipleDistinctReporters() {
        ReachabilityManager r = new ReachabilityManager(3);
        Multiaddr pub = new Multiaddr("/ip4/1.2.3.4/tcp/4001");

        r.observeAddress(pub, peer(1));
        r.observeAddress(pub, peer(1)); // same reporter, no additional confidence
        r.observeAddress(pub, peer(2));
        Assert.assertTrue("not yet enough distinct reporters", r.getCandidateAddresses().isEmpty());

        r.observeAddress(pub, peer(3));
        Assert.assertEquals("promoted after 3 distinct reporters", List.of(pub), r.getCandidateAddresses());
    }

    @Test
    public void privateAddressesAreIgnored() {
        ReachabilityManager r = new ReachabilityManager(1);
        r.observeAddress(new Multiaddr("/ip4/192.168.0.5/tcp/4001"), peer(1));
        r.observeAddress(new Multiaddr("/ip4/127.0.0.1/tcp/4001"), peer(2));
        Assert.assertTrue(r.getAllObservedAddresses().isEmpty());
        Assert.assertTrue(r.getCandidateAddresses().isEmpty());
    }

    @Test
    public void localCandidatesAreOfferedToAutonatButFilteredForPrivacy() {
        ReachabilityManager r = new ReachabilityManager();
        Multiaddr upnpMapped = new Multiaddr("/ip4/1.2.3.4/tcp/4001");
        r.addLocalCandidate(upnpMapped);
        r.addLocalCandidate(new Multiaddr("/ip4/10.0.0.5/tcp/4001")); // private, ignored

        Assert.assertEquals("local UPnP candidate is offered for AutoNAT verification",
                List.of(upnpMapped), r.getAllObservedAddresses());
    }

    @Test
    public void listenerFiresOnlyOnTransition() {
        ReachabilityManager r = new ReachabilityManager();
        AtomicInteger fired = new AtomicInteger();
        AtomicReference<Reachability> last = new AtomicReference<>();
        r.addListener(state -> { fired.incrementAndGet(); last.set(state); });

        Assert.assertEquals(Reachability.UNKNOWN, r.getReachability());
        Multiaddr pub = new Multiaddr("/ip4/1.2.3.4/tcp/4001");

        r.setReachability(Reachability.PUBLIC, List.of(pub));
        r.setReachability(Reachability.PUBLIC, List.of(pub)); // no transition
        Assert.assertEquals(1, fired.get());
        Assert.assertEquals(Reachability.PUBLIC, last.get());
        Assert.assertEquals(List.of(pub), r.getConfirmedPublicAddresses());

        r.setReachability(Reachability.PRIVATE, List.of());
        Assert.assertEquals(2, fired.get());
        Assert.assertTrue(r.getConfirmedPublicAddresses().isEmpty());
    }
}
