package org.peergos;

import io.libp2p.core.multiformats.*;

import java.net.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Leaves IPv6 addresses out of our dials while this host has no route to them. A QUIC dial to one would otherwise
 *  fail inside the transport, whose datagram send throws "Network is unreachable" at the end of the netty
 *  pipeline instead of failing the dial, so each attempt logs a warning per packet until its handshake times out.
 *  Addresses are filtered where we dial them, so the address book keeps them, except for bootstrap peers and
 *  relay candidates, which are filtered as they are chosen. */
public class Ipv6Route {
    // Networks change under a running node (laptops, phones), so the answer is only trusted this long
    private static final long RECHECK_NANOS = TimeUnit.SECONDS.toNanos(30);
    // Any global unicast address will do to find a default route
    private static final InetAddress GLOBAL = new InetSocketAddress("2001:4860:4860::8888", 53).getAddress();
    private static final Ipv6Route HOST = new Ipv6Route(() -> routeTo(GLOBAL), System::nanoTime, Ipv6Route::routeTo);

    private record Check(boolean routed, long at) {}

    private final BooleanSupplier probe;
    private final LongSupplier nanoClock;
    private final Predicate<InetAddress> routeTo;
    private volatile Check last;

    Ipv6Route(BooleanSupplier probe, LongSupplier nanoClock, Predicate<InetAddress> routeTo) {
        this.probe = probe;
        this.nanoClock = nanoClock;
        this.routeTo = routeTo;
    }

    public static boolean isDialable(Multiaddr a) {
        return HOST.dialable(a);
    }

    public static Multiaddr[] dialable(Multiaddr[] addrs) {
        return Arrays.stream(addrs).filter(Ipv6Route::isDialable).toArray(Multiaddr[]::new);
    }

    /** Without a default route, an address can still be reachable over a narrower one, such as a VPN's or an
     *  overlay's prefix, so that address's own route is looked up. A /dns6 name has no address to look up. */
    boolean dialable(Multiaddr a) {
        if (! needsRoute(a) || hasRoute())
            return true;
        MultiaddrComponent ip6 = a.getFirstComponent(Protocol.IP6);
        try {
            return ip6 != null && routeTo.test(InetAddress.getByAddress(ip6.getValue()));
        } catch (UnknownHostException e) {
            return false;
        }
    }

    boolean hasRoute() {
        long now = nanoClock.getAsLong();
        Check c = last;
        if (c == null || now - c.at >= RECHECK_NANOS)
            last = c = new Check(probe.getAsBoolean(), now);
        return c.routed;
    }

    /** /ip6 addresses other than loopback, the wildcard and link-local (on this host or link), and /dns6. ULA
     *  (fc00::/7) counts too: it is routed like a global address, and a host with no route to the IPv6 internet
     *  rarely has one to another network's ULA prefix. A relayed address is judged by its relay's address, the
     *  one we actually dial, so relaying through an IPv4 relay is unaffected. */
    static boolean needsRoute(Multiaddr a) {
        if (a.has(Protocol.DNS6))
            return true;
        MultiaddrComponent ip6 = a.getFirstComponent(Protocol.IP6);
        if (ip6 == null)
            return false;
        try {
            // an IPv4-mapped address comes back as an Inet4Address, which an IPv4 route carries
            InetAddress ip = InetAddress.getByAddress(ip6.getValue());
            return ip instanceof Inet6Address && ! ip.isLoopbackAddress() && ! ip.isAnyLocalAddress()
                    && ! ip.isLinkLocalAddress();
        } catch (UnknownHostException e) {
            return false;
        }
    }

    /** Connecting a UDP socket only looks up a route, nothing is sent */
    private static boolean routeTo(InetAddress ip) {
        try (DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET6)) {
            ch.connect(new InetSocketAddress(ip, 53)); // fails with "Network is unreachable" when there is no route
            return true;
        } catch (Exception e) { // including IPv6 being unsupported or disabled in the JVM
            return false;
        }
    }
}
