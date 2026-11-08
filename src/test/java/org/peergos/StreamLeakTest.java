package org.peergos;

import com.sun.net.httpserver.*;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import io.netty.handler.codec.http.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.net.*;
import org.peergos.protocol.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.autonat.pb.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

/** A host keeps every open stream until it is closed. These requests each left one open, so a node that many
 *  peers ask, e.g. a bootstrap node, built up thousands, and its cpu with them. */
public class StreamLeakTest {

    private static final String KADEMLIA = "/ipfs/kad/1.0.0";
    private static final String PING = "/ipfs/ping/1.0.0";
    private static final String HTTP = "/http";

    private static long openStreams(Host h, String protocol) {
        return h.getStreams().stream()
                .filter(s -> protocol.equals(s.getProtocol().getNow(null)))
                .count();
    }

    private static boolean waitUntil(BooleanSupplier condition, long millis) throws InterruptedException {
        long end = System.currentTimeMillis() + millis;
        while (System.currentTimeMillis() < end) {
            if (condition.getAsBoolean())
                return true;
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }

    private static class AutonatPair {
        final int clientPort = TestPorts.getPort(), serverPort = TestPorts.getPort();
        final NonceRegistry clientNonces = new NonceRegistry();
        final AutonatV2DialBack.Binding clientDialBack = new AutonatV2DialBack.Binding(clientNonces);
        final AutonatV2.Binding clientV2 = new AutonatV2.Binding(clientDialBack);
        final HostBuilder clientBuilder = new HostBuilder(new RamAddressBook()).generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + clientPort)))
                .addProtocols(List.of(new IdleTimeoutPing(), clientV2, clientDialBack));
        final Host client = clientBuilder.build();
        final Host server;
        final Connection conn;

        AutonatPair() throws Exception {
            List<ProtocolBinding> serverProtocols = new ArrayList<>(List.of(new IdleTimeoutPing()));
            serverProtocols.addAll(AutonatV2.protocols());
            server = new HostBuilder(new RamAddressBook()).generateIdentity()
                    .listen(List.of(new MultiAddress("/ip4/127.0.0.1/tcp/" + serverPort)))
                    .addProtocols(serverProtocols)
                    .build();
            client.start().join();
            server.start().join();
            conn = client.getNetwork()
                    .connect(server.getPeerId(), new Multiaddr("/ip4/127.0.0.1/tcp/" + serverPort))
                    .get(15, TimeUnit.SECONDS);
        }

        Autonatv2.DialResponse requestDial(Multiaddr addr) throws Exception {
            long nonce = ThreadLocalRandom.current().nextLong();
            // expected, so a dial back is acknowledged rather than the server waiting out its timeout
            clientNonces.expect(nonce);
            try {
                return conn.muxerSession().createStream(clientV2).getController()
                        .thenCompose(c -> c.requestDial(List.of(addr), nonce))
                        .get(30, TimeUnit.SECONDS);
            } finally {
                clientNonces.forget(nonce);
            }
        }

        void stop() {
            client.stop();
            server.stop();
        }
    }

    @Test
    public void autonatDialRequestsCloseTheirStreams() throws Exception {
        AutonatPair pair = new AutonatPair();
        try {
            Multiaddr ours = new Multiaddr("/ip4/127.0.0.1/tcp/" + pair.clientPort);
            for (int i = 0; i < 10; i++)
                Assert.assertEquals(Autonatv2.DialResponse.ResponseStatus.OK, pair.requestDial(ours).getStatus());
            Assert.assertTrue("no dial request streams left open",
                    waitUntil(() -> openStreams(pair.client, AutonatV2.PROTOCOL) == 0
                            && openStreams(pair.server, AutonatV2.PROTOCOL) == 0, 5_000));
        } finally {
            pair.stop();
        }
    }

    @Test
    public void autonatServerRateLimitsEachPeer() throws Exception {
        AutonatPair pair = new AutonatPair();
        try {
            // not on the address the server sees us at, so it refuses to dial without waiting on a dial back
            Multiaddr elsewhere = new Multiaddr("/ip4/10.9.9.9/tcp/4001");
            int rejected = 0, answered = 0;
            for (int i = 0; i < 15; i++) {
                Autonatv2.DialResponse resp = pair.requestDial(elsewhere);
                if (resp.getStatus() == Autonatv2.DialResponse.ResponseStatus.E_REQUEST_REJECTED)
                    rejected++;
                else
                    answered++;
            }
            Assert.assertEquals("a peer's requests in a minute are capped", 12, answered);
            Assert.assertEquals(3, rejected);
            Assert.assertTrue("rejected requests close their streams too",
                    waitUntil(() -> openStreams(pair.server, AutonatV2.PROTOCOL) == 0, 5_000));
        } finally {
            pair.stop();
        }
    }

    @Test
    public void autonatClientBacksOffAPeerThatCanNotDecide() throws Exception {
        AutonatPair pair = new AutonatPair();
        try {
            AtomicInteger requests = new AtomicInteger();
            pair.server.addStreamVisitor(s -> s.getProtocol().thenAccept(p -> {
                if (AutonatV2.PROTOCOL.equals(p))
                    requests.incrementAndGet();
            }));
            // like a node behind nat: its candidate isn't on the address the server sees, so every answer is a refusal
            Multiaddr elsewhere = new Multiaddr("/ip4/10.9.9.9/tcp/4001");
            AutoNatV2Client autonat = new AutoNatV2Client(pair.client, pair.clientBuilder.getReachability(),
                    pair.clientV2, pair.clientNonces, () -> List.of(elsewhere), 1);
            autonat.onConnection(pair.conn);
            Assert.assertTrue(waitUntil(() -> requests.get() == 1, 5_000));
            Thread.sleep(500);
            for (int i = 0; i < 5; i++) {
                autonat.probePending();
                Thread.sleep(200);
            }
            Assert.assertEquals("asked once, not on every poll", 1, requests.get());
        } finally {
            pair.stop();
        }
    }

    private static Host host(ProtocolBinding... extra) {
        HostBuilder builder = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        for (ProtocolBinding b : extra)
            builder.addProtocol(b);
        Host h = builder.build();
        h.start().join();
        return h;
    }

    private static Connection connect(Host from, Host to) throws Exception {
        return from.getNetwork().connect(to.getPeerId(), to.listenAddresses().get(0)).get(15, TimeUnit.SECONDS);
    }

    @Test
    public void kademliaStreamsWithoutARequestAreClosed() throws Exception {
        int idle = StreamTimeouts.INCOMING_IDLE_SECONDS;
        StreamTimeouts.INCOMING_IDLE_SECONDS = 1;
        HostBuilder clientBuilder = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true));
        Host client = clientBuilder.build();
        client.start().join();
        Host server = host();
        try {
            Connection conn = connect(client, server);
            conn.muxerSession().createStream(clientBuilder.getWanDht().get()).getController().get(10, TimeUnit.SECONDS);
            Assert.assertTrue(waitUntil(() -> openStreams(server, KADEMLIA) == 1, 5_000));
            Assert.assertTrue("a stream that never sends a request is closed",
                    waitUntil(() -> openStreams(server, KADEMLIA) == 0, 5_000));
        } finally {
            StreamTimeouts.INCOMING_IDLE_SECONDS = idle;
            client.stop();
            server.stop();
        }
    }

    @Test
    public void idlePingStreamsAreClosedButBusyOnesAreNot() throws Exception {
        int idle = StreamTimeouts.INCOMING_IDLE_SECONDS;
        StreamTimeouts.INCOMING_IDLE_SECONDS = 1;
        Host client = host();
        Host server = host();
        try {
            Connection conn = connect(client, server);
            PingController ping = conn.muxerSession().createStream(new IdleTimeoutPing()).getController().get(10, TimeUnit.SECONDS);
            for (int i = 0; i < 8; i++) {
                ping.ping().get(5, TimeUnit.SECONDS);
                Thread.sleep(300);
            }
            Assert.assertEquals("a stream in use stays open", 1, openStreams(server, PING));
            Assert.assertTrue("an unused one is closed",
                    waitUntil(() -> openStreams(server, PING) == 0, 5_000));
        } finally {
            StreamTimeouts.INCOMING_IDLE_SECONDS = idle;
            client.stop();
            server.stop();
        }
    }

    @Test
    public void httpRequestsWhoseReplyIsDroppedCloseTheirStreams() throws Exception {
        InetSocketAddress unused = new InetSocketAddress("127.0.0.1", 7000);
        InetSocketAddress targetHttp = new InetSocketAddress("127.0.0.1", TestPorts.getPort());
        HostBuilder proxyBuilder = HostBuilder.create(TestPorts.getPort(),
                new RamProviderStore(1000), new RamRecordStore(), new RamBlockstore(), (c, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(unused));
        Host proxy = proxyBuilder.build();
        proxy.start().join();
        Host target = host(new HttpProtocol.Binding(targetHttp));
        HttpServer server = HttpServer.create(targetHttp, 20);
        // closes the connection without responding
        server.createContext("/drop", HttpExchange::close);
        server.start();
        try {
            proxy.getAddressBook().setAddrs(target.getPeerId(), 0, target.listenAddresses().get(0)).join();
            HttpProxyService service = new HttpProxyService(proxy, new HttpProtocol.Binding(unused), proxyBuilder.getWanDht().get());
            Multihash targetId = Multihash.deserialize(target.getPeerId().getBytes());
            for (int i = 0; i < 5; i++) {
                ProxyResponse resp = CompletableFuture.supplyAsync(() -> {
                    try {
                        return service.proxyRequest(targetId, new ProxyRequest("/drop", ProxyRequest.Method.GET,
                                Collections.emptyMap(), Collections.emptyMap(), null));
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }).get(10, TimeUnit.SECONDS);
                Assert.assertEquals(502, resp.statusCode);
            }
            Assert.assertTrue(waitUntil(() -> openStreams(proxy, HTTP) == 0 && openStreams(target, HTTP) == 0, 5_000));
        } finally {
            server.stop(0);
            proxy.stop();
            target.stop();
        }
    }

    @Test
    public void httpRequestsWithNoReplyTimeOut() throws Exception {
        int timeout = StreamTimeouts.HTTP_RESPONSE_SECONDS;
        StreamTimeouts.HTTP_RESPONSE_SECONDS = 1;
        InetSocketAddress unused = new InetSocketAddress("127.0.0.1", 7000);
        Host proxy = host(new HttpProtocol.Binding(unused));
        // a peer that never replies
        Host target = host(new HttpProtocol.Binding((stream, msg, reply) -> {}));
        try {
            HttpProtocol.HttpController sender = new HttpProtocol.Binding(unused)
                    .dial(proxy, target.listenAddresses().get(0)).getController().get(10, TimeUnit.SECONDS);
            try {
                sender.send(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")).get(10, TimeUnit.SECONDS);
                Assert.fail("a request with no reply should fail");
            } catch (ExecutionException expected) {
                Assert.assertTrue(expected.getCause() instanceof TimeoutException);
            }
            Assert.assertTrue(waitUntil(() -> openStreams(proxy, HTTP) == 0, 5_000));
        } finally {
            StreamTimeouts.HTTP_RESPONSE_SECONDS = timeout;
            proxy.stop();
            target.stop();
        }
    }
}
