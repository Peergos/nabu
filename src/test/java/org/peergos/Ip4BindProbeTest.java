package org.peergos;

import io.ipfs.multiaddr.*;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.libp2p.protocol.*;
import org.junit.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Diagnostic: when we ask to listen on the IPv4 wildcard, does the node actually accept an inbound
 * IPv4 QUIC/TCP connection, or does it end up only bound to the IPv6 wildcard (::) - which would make a
 * fixed-public-IPv4 host look PRIVATE to AutoNAT because dial-backs to its v4 address are refused.
 */
public class Ip4BindProbeTest {

    private static HostBuilder node(String listen) {
        return new HostBuilder(new RamAddressBook()).generateIdentity()
                .addProtocols(List.of(new Ping()))
                .listen(List.of(new MultiAddress(listen)));
    }

    private void probe(String proto, String listenTemplate) throws Exception {
        int port = TestPorts.getPort();
        String listen = listenTemplate.replace("PORT", "" + port);
        Host server = node(listen).build();
        server.start().get(10, TimeUnit.SECONDS);
        System.out.println("[" + proto + "] asked to listen on " + listen);
        System.out.println("[" + proto + "] actually listening on " + server.listenAddresses());

        Host client = node(listenTemplate.replace("PORT", "" + TestPorts.getPort())).build();
        client.start().get(10, TimeUnit.SECONDS);

        Multiaddr dialV4 = new Multiaddr(listen.replace("0.0.0.0", "127.0.0.1")).withP2P(server.getPeerId());
        System.out.println("[" + proto + "] dialing v4 " + dialV4);
        String result;
        try {
            PingController pinger = new Ping().dial(client, dialV4).getController().get(10, TimeUnit.SECONDS);
            long rtt = pinger.ping().get(10, TimeUnit.SECONDS);
            result = "SUCCESS rtt=" + rtt + "ms";
        } catch (Exception e) {
            result = "FAILED: " + e.getClass().getSimpleName() + " " + e.getMessage();
        }
        System.out.println("[" + proto + "] inbound IPv4 dial -> " + result);
        client.stop().get(5, TimeUnit.SECONDS);
        server.stop().get(5, TimeUnit.SECONDS);
        Assert.assertTrue("[" + proto + "] inbound IPv4 dial to a 0.0.0.0 listener: " + result,
                result.startsWith("SUCCESS"));
    }

    @Test
    public void quicWildcardAcceptsIpv4() throws Exception {
        probe("quic", "/ip4/0.0.0.0/udp/PORT/quic-v1");
    }
}
