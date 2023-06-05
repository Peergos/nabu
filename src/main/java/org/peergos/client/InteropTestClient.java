package org.peergos.client;

import identify.pb.IdentifyOuterClass;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.dsl.Builder;
import io.libp2p.core.dsl.BuilderJKt;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.ProtocolBinding;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.crypto.keys.Ed25519Kt;
import io.libp2p.etc.types.ByteArrayExtKt;
import io.libp2p.protocol.Identify;
import io.libp2p.protocol.Ping;
import io.libp2p.protocol.PingController;
import io.libp2p.security.noise.NoiseXXSecureChannel;
import io.libp2p.security.tls.TlsSecureChannel;
import io.libp2p.transport.tcp.TcpTransport;
import io.ipfs.multiaddr.MultiAddress;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InteropTestClient {

    private class RamAddressBook implements AddressBook {

        Map<PeerId, Set<Multiaddr>> addresses = new ConcurrentHashMap<>();

        @NotNull
        @Override
        public CompletableFuture<Void> addAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
            addresses.putIfAbsent(peerId, new HashSet<>());
            Set<Multiaddr> val = addresses.get(peerId);
            synchronized (val) {
                val.addAll(Arrays.asList(multiaddrs));
            }
            return CompletableFuture.completedFuture(null);
        }

        @NotNull
        @Override
        public CompletableFuture<Collection<Multiaddr>> getAddrs(@NotNull PeerId peerId) {
            return CompletableFuture.completedFuture(addresses.getOrDefault(peerId, Collections.emptySet()));
        }

        @NotNull
        @Override
        public CompletableFuture<Void> setAddrs(@NotNull PeerId peerId, long ttl, @NotNull Multiaddr... multiaddrs) {
            Set<Multiaddr> val = new HashSet<>();
            val.addAll(Arrays.asList(multiaddrs));
            addresses.put(peerId, val);
            return CompletableFuture.completedFuture(null);
        }
    }

    private static String getLocalIPAddress() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("google.com", 80));
            return socket.getLocalAddress().getHostAddress();
        } catch (IOException ioe){
            throw new IllegalStateException("Unable to determine local IPAddress");
        }
    }
    private InteropTestClient(String transport, String muxer, String security, boolean is_dialer,
                              String ip, String redis_addr, String test_timeout_seconds) throws Exception {
        if (ip == null || ip.length() == 0) {
            if (is_dialer) {
                ip = "0.0.0.0";
            } else {
                ip = getLocalIPAddress();
            }
        }
        if (redis_addr == null || redis_addr.length() == 0) {
            redis_addr = "redis:6379";
        }
        if (test_timeout_seconds == null || test_timeout_seconds.length() == 0) {
            test_timeout_seconds = "180";
        }
        System.err.println("Parameters: transport=" + transport + " muxer=" + muxer +
                " security=" + security + " is_dialer=" + is_dialer + " ip=" + ip +
                " redis_addr=" + redis_addr + " test_timeout_seconds=" + test_timeout_seconds);
        if (transport == null ||  muxer == null || security == null) {
            throw new IllegalStateException("transport == null ||  muxer == null || security == null");
        }
        int port = 10000 + new Random().nextInt(50000);
        //.listen(List.of(new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort)))
        Multiaddr address = Multiaddr.fromString("/ip4/" + ip + "/tcp/" + port);

        List<MultiAddress> swarmAddresses = List.of(new MultiAddress(address.toString()));

        List<ProtocolBinding> protocols = new ArrayList<>();
        protocols.add(new Ping());

        // HostBuilder builder =  new HostBuilder().generateIdentity();
        PrivKey privKey = Ed25519Kt.generateEd25519KeyPair().getFirst();
        PeerId peerId = PeerId.fromPubKey(privKey.publicKey());
        Multiaddr advertisedAddr = address.withP2P(peerId);
        List<String> listenAddrs = new ArrayList<>();
        listenAddrs.addAll(swarmAddresses.stream().map(MultiAddress::toString).collect(Collectors.toList()));
        Host node = BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            if (transport.equals("tcp")) {
                b.getTransports().add(TcpTransport::new);
            }
            if (security.equals("noise")) {
                b.getSecureChannels().add((k, m) -> new NoiseXXSecureChannel(k, m));
            } else if(security.equals("tls")) {
                b.getSecureChannels().add((k, m) -> new TlsSecureChannel(k, m));
            }
            List<StreamMuxerProtocol> muxers = new ArrayList<>();
            if (muxer.equals("mplex")) {
                muxers.add(StreamMuxerProtocol.getMplex());
            } else if(muxer.equals("yamux")) {
                muxers.add(StreamMuxerProtocol.getYamux());
            }
            b.getMuxers().addAll(muxers);
            b.getAddressBook().setImpl(new RamAddressBook());

            for (ProtocolBinding<?> protocol : protocols) {
                b.getProtocols().add(protocol);
            }

            IdentifyOuterClass.Identify.Builder identifyBuilder = IdentifyOuterClass.Identify.newBuilder()
                    .setProtocolVersion("ipfs/0.1.0")
                    .setAgentVersion("nabu/v0.1.0")
                    .setPublicKey(ByteArrayExtKt.toProtobuf(privKey.publicKey().bytes()))
                    .addAllListenAddrs(listenAddrs.stream()
                            .map(Multiaddr::fromString)
                            .map(Multiaddr::serialize)
                            .map(ByteArrayExtKt::toProtobuf)
                            .collect(Collectors.toList()));
            for (ProtocolBinding<?> protocol : protocols) {
                identifyBuilder = identifyBuilder.addAllProtocols(protocol.getProtocolDescriptor().getAnnounceProtocols());
            }
            b.getProtocols().add(new Identify(identifyBuilder.build()));

            for (String listenAddr : listenAddrs) {
                b.getNetwork().listen(listenAddr);
            }

            b.getConnectionHandlers().add(conn -> System.err.println(conn.localAddress() +
                    " received connection from " + conn.remoteAddress() +
                    " on transport " + conn.transport()));
        });
        node.start().join();
        Jedis jedis = new Jedis("http://" + redis_addr);
        boolean isReady = false;
        while (!isReady) {
            if ("PONG".equals(jedis.ping())) {
                isReady = true;
            } else {
                System.err.println("waiting for redis to start...");
                Thread.sleep(2000);
            }
        }
        System.err.println("My multiaddr is: " + advertisedAddr);
        try {
            if (is_dialer) {
                List<String> listenerAddrs = jedis.blpop(Integer.parseInt(test_timeout_seconds), "listenerAddr");
                if (listenerAddrs == null || listenerAddrs.isEmpty()) {
                    throw new IllegalStateException("listenerAddr not set");
                }
                String listenerAddrStr = listenerAddrs.get(listenerAddrs.size() -1);// jedis.get("listenerAddr");
                Multiaddr listenerAddr = Multiaddr.fromString(listenerAddrStr);
                System.err.println("Other peer multiaddr is: " + listenerAddr);
                try {
                    System.err.println("Sending ping messages to " + listenerAddr);
                    long handshakeStart = System.currentTimeMillis();
                    PingController pinger = new Ping().dial(node, listenerAddr).getController().join();
                    long pingRTTMilllis = pinger.ping().join();
                    long handshakeEnd = System.currentTimeMillis();
                    long handshakePlusOneRTT = handshakeEnd - handshakeStart;
                    System.err.println("Ping latency " + pingRTTMilllis + "ms");
                    String jsonResult = "{\"handshakePlusOneRTTMillis\":" + Double.valueOf(handshakePlusOneRTT) +
                            ",\"pingRTTMilllis\":" + Double.valueOf(pingRTTMilllis) + "}";
                    System.out.println(jsonResult);
                } finally {
                    node.stop();
                }
            } else {
                //jedis.set("listenerAddr", advertisedAddr.toString());
                //_, err := rClient.RPush(ctx, "listenerAddr", host.Addrs()[0].Encapsulate(ma.StringCast("/p2p/"+host.ID().String())).String()).Result()
                jedis.rpush("listenerAddr", advertisedAddr.toString());
                Thread.sleep(Integer.parseInt(test_timeout_seconds) * 1000);
            }
        } finally {
            System.err.println("Shutting down");
            node.stop();
        }
    }
    public static void main(String[] args) {
        try {
            String transport = System.getenv("transport"); //"tcp"
            String muxer = System.getenv("muxer"); //"mplex", "yamux"
            String security = System.getenv("security"); //"tls", "noise"
            boolean is_dialer = Boolean.parseBoolean(System.getenv("is_dialer"));
            String ip = System.getenv("ip");
            String redis_addr = System.getenv("redis_addr");
            String test_timeout_seconds = System.getenv("test_timeout_seconds");
            /*
            String transport = "tcp";
            String muxer = "yamux";//, "yamux"
            String security = "noise";//, "noise"
            boolean is_dialer = true;
            String ip = null;
            String redis_addr = "127.0.0.1:6379";
            String test_timeout_seconds = "180";*/
            new InteropTestClient(transport, muxer, security, is_dialer, ip, redis_addr, test_timeout_seconds);
        } catch (Throwable t) {
            System.err.println("Unexpected exit: " + t);
            System.exit(-1);
        }
    }
}
