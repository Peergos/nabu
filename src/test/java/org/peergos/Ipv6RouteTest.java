package org.peergos;

import io.libp2p.core.multiformats.*;
import org.junit.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

public class Ipv6RouteTest {
    private static final String PEER = "/p2p/12D3KooWD3eckifWpRn9wQpMG9R9hX3sD158z7EqHWmweQAJU5SA";
    private static final Multiaddr IP4 = new Multiaddr("/ip4/1.2.3.4/udp/4001/quic-v1");
    private static final Multiaddr GLOBAL = new Multiaddr("/ip6/2001:db8::1/udp/4001/quic-v1");
    private static final Multiaddr LOOPBACK = new Multiaddr("/ip6/::1/udp/4001/quic-v1");
    private static final Multiaddr RELAYED4 = new Multiaddr("/ip4/1.2.3.4/udp/4001/quic-v1" + PEER + "/p2p-circuit" + PEER);
    private static final Multiaddr RELAYED6 = new Multiaddr("/ip6/2001:db8::1/udp/4001/quic-v1" + PEER + "/p2p-circuit" + PEER);

    private static boolean needsRoute(String addr) {
        return Ipv6Route.needsRoute(new Multiaddr(addr));
    }

    @Test
    public void addressesThatNeedARoute() {
        Assert.assertTrue(needsRoute("/ip6/2001:db8::1/tcp/4001"));
        Assert.assertTrue(needsRoute("/ip6/2001:db8::1/udp/4001/quic-v1" + PEER));
        Assert.assertTrue(needsRoute("/ip6/fd12:3456::1/tcp/4001")); // ULA
        Assert.assertTrue(needsRoute("/dns6/example.com/tcp/4001"));
        Assert.assertTrue(Ipv6Route.needsRoute(RELAYED6));

        Assert.assertFalse(needsRoute("/ip4/1.2.3.4/tcp/4001"));
        Assert.assertFalse(needsRoute("/dns4/example.com/tcp/4001"));
        Assert.assertFalse(needsRoute("/dns/example.com/tcp/4001"));
        Assert.assertFalse(needsRoute("/ip6/::1/tcp/4001"));
        Assert.assertFalse(needsRoute("/ip6/::/tcp/4001")); // a wildcard listen address, which dials this host
        Assert.assertFalse(needsRoute("/ip6/fe80::1/tcp/4001"));
        Assert.assertFalse(Ipv6Route.needsRoute(RELAYED4));
        Assert.assertFalse(needsRoute(PEER + "/p2p-circuit" + PEER));
        // IPv4-mapped, which only arrives in binary form: the string parser rejects it
        byte[] mapped = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xff, (byte) 0xff, 1, 2, 3, 4};
        Assert.assertFalse(Ipv6Route.needsRoute(new Multiaddr(List.of(new MultiaddrComponent(Protocol.IP6, mapped)))));
    }

    @Test
    public void onlyRoutedIpv6IsDroppedWithoutARoute() {
        List<Multiaddr> all = List.of(IP4, GLOBAL, LOOPBACK, RELAYED4, RELAYED6);
        Ipv6Route noRoute = new Ipv6Route(() -> false, () -> 0, ip -> false);
        Ipv6Route routed = new Ipv6Route(() -> true, () -> 0, ip -> false);
        Assert.assertEquals(List.of(IP4, LOOPBACK, RELAYED4), all.stream().filter(noRoute::dialable).collect(Collectors.toList()));
        Assert.assertEquals(all, all.stream().filter(routed::dialable).collect(Collectors.toList()));
    }

    @Test
    public void probesOnlyForIpv6AndRechecks() {
        AtomicInteger probes = new AtomicInteger();
        AtomicBoolean route = new AtomicBoolean(false);
        AtomicLong now = new AtomicLong(-5); // nanoTime may be negative
        Ipv6Route r = new Ipv6Route(() -> { probes.incrementAndGet(); return route.get(); }, now::get, ip -> false);

        Assert.assertTrue(r.dialable(IP4));
        Assert.assertEquals("no probe for an IPv4 dial", 0, probes.get());

        Assert.assertFalse(r.dialable(GLOBAL));
        Assert.assertFalse(r.dialable(GLOBAL));
        Assert.assertEquals("answer is cached", 1, probes.get());

        route.set(true); // e.g. a laptop joins an IPv6 network
        now.addAndGet(TimeUnit.SECONDS.toNanos(29));
        Assert.assertFalse(r.dialable(GLOBAL));
        now.addAndGet(TimeUnit.SECONDS.toNanos(1));
        Assert.assertTrue(r.dialable(GLOBAL));
        Assert.assertEquals(2, probes.get());

        route.set(false); // and leaves it
        now.addAndGet(TimeUnit.SECONDS.toNanos(30));
        Assert.assertFalse(r.dialable(GLOBAL));
        Assert.assertEquals(3, probes.get());
    }

    @Test
    public void withoutADefaultRouteAnAddressWithItsOwnRouteIsKept() {
        // e.g. two peers on an IPv6-only VPN or overlay, whose prefix is routed but nothing else is
        Multiaddr vpn = new Multiaddr("/ip6/fd7a:115c:a1e0::1/udp/4001/quic-v1");
        Multiaddr dns6 = new Multiaddr("/dns6/example.com/udp/4001/quic-v1");
        List<InetAddress> looked = new ArrayList<>();
        Ipv6Route r = new Ipv6Route(() -> false, () -> 0, ip -> {
            looked.add(ip);
            return ip.getHostAddress().startsWith("fd7a:");
        });
        Assert.assertTrue(r.dialable(vpn));
        Assert.assertFalse(r.dialable(GLOBAL));
        Assert.assertFalse("a name has no address to look up", r.dialable(dns6));
        Assert.assertTrue(r.dialable(IP4));
        Assert.assertEquals("only IPv6 addresses are looked up", 2, looked.size());

        Ipv6Route routed = new Ipv6Route(() -> true, () -> 0, ip -> { throw new AssertionError("looked up " + ip); });
        Assert.assertTrue("with a default route nothing is looked up", routed.dialable(GLOBAL));
    }
}
