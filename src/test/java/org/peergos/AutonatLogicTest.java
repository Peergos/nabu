package org.peergos;

import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.autonat.ReachabilityManager.Reachability;

import java.util.*;

public class AutonatLogicTest {

    @Test
    public void onlyDialsObservedIpPublicAddresses() {
        Multiaddr matchingPublic = new Multiaddr("/ip4/1.2.3.4/tcp/4001");
        Multiaddr otherPublic = new Multiaddr("/ip4/5.6.7.8/tcp/4001"); // different IP than observed
        Multiaddr privateAddr = new Multiaddr("/ip4/192.168.0.9/tcp/4001");
        List<Multiaddr> claimed = List.of(matchingPublic, otherPublic, privateAddr);

        List<Multiaddr> selected = AutonatProtocol.selectDialBackAddresses(claimed, "1.2.3.4", 3);
        Assert.assertEquals("only the observed public IP is dialable", List.of(matchingPublic), selected);
    }

    @Test
    public void selectionIsCappedAndDeduped() {
        Multiaddr a = new Multiaddr("/ip4/1.2.3.4/tcp/4001");
        Multiaddr b = new Multiaddr("/ip4/1.2.3.4/tcp/4002");
        Multiaddr c = new Multiaddr("/ip4/1.2.3.4/tcp/4003");
        Multiaddr d = new Multiaddr("/ip4/1.2.3.4/tcp/4004");
        List<Multiaddr> claimed = List.of(a, a, b, c, d); // duplicate a

        List<Multiaddr> selected = AutonatProtocol.selectDialBackAddresses(claimed, "1.2.3.4", 3);
        Assert.assertEquals(3, selected.size());
        Assert.assertEquals(new HashSet<>(selected).size(), selected.size());
    }

    @Test
    public void noObservedIpMeansNoDialBack() {
        List<Multiaddr> claimed = List.of(new Multiaddr("/ip4/1.2.3.4/tcp/4001"));
        Assert.assertTrue(AutonatProtocol.selectDialBackAddresses(claimed, null, 3).isEmpty());
    }

    @Test
    public void verdictNeedsQuorum() {
        Assert.assertEquals(Optional.empty(), AutoNatClient.evaluate(List.of(true, true), 3));
        Assert.assertEquals(Optional.of(Reachability.PUBLIC), AutoNatClient.evaluate(List.of(true, true, true), 3));
        Assert.assertEquals(Optional.of(Reachability.PRIVATE), AutoNatClient.evaluate(List.of(false, false, false), 3));
        // mixed votes, neither side reaches quorum
        Assert.assertEquals(Optional.empty(), AutoNatClient.evaluate(List.of(true, false, true, false), 3));
    }
}
