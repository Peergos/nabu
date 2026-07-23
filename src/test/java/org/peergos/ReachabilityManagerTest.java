package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class ReachabilityManagerTest {

    private static Multihash peer(int i) {
        byte[] raw = new byte[32];
        raw[0] = (byte) i;
        return new Multihash(Multihash.Type.sha2_256, raw);
    }

    /** Attach a handler that collects every message logged through the shared nabu logger. */
    private static List<String> captureLog() {
        List<String> messages = new CopyOnWriteArrayList<>();
        Logger log = org.peergos.util.Logging.LOG();
        Handler handler = new Handler() {
            public void publish(LogRecord record) { messages.add(record.getMessage()); }
            public void flush() {}
            public void close() {}
        };
        handler.setLevel(Level.ALL);
        log.addHandler(handler);
        if (log.getLevel() == null || log.getLevel().intValue() > Level.INFO.intValue())
            log.setLevel(Level.INFO);
        return messages;
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
    public void observedHostsAggregatePortsButNeedAQuorum() {
        ReachabilityManager r = new ReachabilityManager();
        // same IP observed by 3 peers, each at a different (ephemeral) port -> IP still qualifies
        r.observeAddress(new Multiaddr("/ip4/1.2.3.4/tcp/30001"), peer(1));
        r.observeAddress(new Multiaddr("/ip4/1.2.3.4/udp/41999/quic-v1"), peer(2));
        r.observeAddress(new Multiaddr("/ip4/1.2.3.4/tcp/55123"), peer(3));
        r.observeAddress(new Multiaddr("/ip4/9.9.9.9/tcp/4001"), peer(4)); // only one reporter
        Assert.assertEquals(Set.of("1.2.3.4"), r.getObservedHosts(3));
    }

    @Test
    public void natTypeInferredFromHolePunchOutcomes() {
        ReachabilityManager r = new ReachabilityManager();
        Assert.assertEquals(ReachabilityManager.NatType.UNKNOWN, r.getNatType());
        // failures to distinct peers -> symmetric
        r.recordHolePunchOutcome(peer(1), false);
        Assert.assertEquals(ReachabilityManager.NatType.UNKNOWN, r.getNatType());
        r.recordHolePunchOutcome(peer(2), false);
        Assert.assertEquals(ReachabilityManager.NatType.SYMMETRIC, r.getNatType());
        // a success flips it to endpoint-independent
        r.recordHolePunchOutcome(peer(3), true);
        Assert.assertEquals(ReachabilityManager.NatType.ENDPOINT_INDEPENDENT, r.getNatType());
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

    @Test
    public void logsConfirmedPublicAddressesAndReLogsWhenExternalIpChanges() {
        List<String> logged = captureLog();
        ReachabilityManager r = new ReachabilityManager();
        Multiaddr pub = new Multiaddr("/ip4/1.2.3.4/udp/4001/quic-v1");

        r.setReachability(Reachability.PUBLIC, List.of(pub));
        Assert.assertTrue("the confirmed public address is logged",
                logged.stream().anyMatch(m -> m.contains("NAT traversal status")
                        && m.contains("reachability=PUBLIC") && m.contains("1.2.3.4")));

        int afterFirst = logged.size();
        r.setReachability(Reachability.PUBLIC, List.of(pub)); // identical verdict and address
        Assert.assertEquals("an unchanged status is not re-logged", afterFirst, logged.size());

        // Same PUBLIC verdict but a new external IP: this must still produce a log line.
        Multiaddr movedTo = new Multiaddr("/ip4/5.6.7.8/udp/4001/quic-v1");
        r.setReachability(Reachability.PUBLIC, List.of(movedTo));
        Assert.assertTrue("a changed external IP is logged even without a verdict transition",
                logged.stream().anyMatch(m -> m.contains("5.6.7.8")));
    }
}
