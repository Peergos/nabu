package org.peergos.client;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.Stream;
import io.libp2p.core.StreamPromise;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.dsl.Builder;
import io.libp2p.core.dsl.BuilderJKt;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.crypto.keys.Ed25519Kt;
import io.libp2p.security.noise.NoiseXXSecureChannel;
import io.libp2p.transport.tcp.TcpTransport;
import org.peergos.HostBuilder;
import org.peergos.protocol.perf.Perf;
import org.peergos.protocol.perf.PerfController;
import org.peergos.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class PerfTest2 {

    private Pair<HostBuilder, Optional<PeerId>> buildHost(boolean isServer) {
        String base64PrivKeyStr = "CAESIPtYePRNDRG9RrQ06shBER+MVUyEFJHtrFMiNgabSj1j";
        byte[] privKeyBytes = io.ipfs.multibase.binary.Base64.decodeBase64(base64PrivKeyStr);
        HostBuilder builder = new HostBuilder().setIdentity(privKeyBytes);
        Optional<PeerId> serverPeerId = Optional.empty();
        if (!isServer) {
            serverPeerId = Optional.of(builder.getPeerId());
            PrivKey privKey = Ed25519Kt.generateEd25519KeyPair().getFirst();
            builder = new HostBuilder().setIdentity(privKey.bytes());
        }
        return new Pair<>(builder, serverPeerId);
    }

    private PerfTest2(boolean isServer, String otherServerAddress, String transport,
                      int uploadBytes, int downloadBytes) throws Exception {
        if (!transport.equals("tcp")) {
            throw new IllegalArgumentException("Invalid transport protocol: " + transport);
        }
        int port = 10000 + new Random().nextInt(50000);
        String[] serverAddressParts = !isServer ? otherServerAddress.split(":") :
                new String[]{"127.0.0.1", "" + port};
        if (!isServer && serverAddressParts.length != 2) {
            throw new IllegalArgumentException("Invalid serverAddress! Expecting - <host>:<port>");
        }
        String listenAddress = "/ip4/" + serverAddressParts[0] + "/tcp/" + serverAddressParts[1];

        Pair<HostBuilder, Optional<PeerId>> hostPair = buildHost(isServer);
        HostBuilder serverBuilder = hostPair.left;
        PrivKey privKey = serverBuilder.getPrivateKey();
        Host node = BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add((k, m) -> new NoiseXXSecureChannel(k, m));

            List<StreamMuxerProtocol> muxers = new ArrayList<>();
            muxers.add(StreamMuxerProtocol.getYamux());
            b.getMuxers().addAll(muxers);
            if (isServer) {
                b.getProtocols().add(new Perf(uploadBytes, downloadBytes));

                Multiaddr serverMultiAddr = Multiaddr.fromString(listenAddress);
                b.getNetwork().listen(serverMultiAddr.toString());
                b.getConnectionHandlers().add(conn -> System.err.println(conn.localAddress() +
                        " received connection from " + conn.remoteAddress() +
                        " on transport " + conn.transport()));
            }
        });
        node.start().join();
        if (isServer) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.err.println("Shutting down server");
                node.stop();
            }));
        }
        if (isServer) {
            System.err.println("Server started. Address: " + listenAddress + " waiting for connection...");
        } else {
            PeerId otherHostPeerId = hostPair.right.get();
            StreamPromise<PerfController> perf = node
                .getNetwork()
                .connect(otherHostPeerId, new Multiaddr(listenAddress))
                .thenApply(it -> it.muxerSession().createStream(new Perf(uploadBytes, downloadBytes)))
                .join();

            Stream perfStream = perf.getStream().get(5, TimeUnit.SECONDS);
            PerfController perfCtr = perf.getController().get(5, TimeUnit.SECONDS);

            double latency = perfCtr.perf().join() / 1000.0;
            perfStream.close().get(5, TimeUnit.SECONDS);
            node.stop();
            System.out.println("{\"type\": \"final\",\"timeSeconds\": " + latency + ",\"uploadBytes\": " + uploadBytes
                    + ",\"downloadBytes\": " + downloadBytes + "}");
        }
    }
    public static void main(String[] args) {
        try {
            boolean isServer = false;//Boolean.parseBoolean(System.getenv("run-server"));
            String serverAddress = "127.0.0.1:32996";//System.getenv("server-address");
            String transport = "tcp";//System.getenv("transport");
            int uploadBytes = 10 * 1024 * 1024; //Integer.parseInt("1");//System.getenv("upload-bytes");
            int downloadBytes = 5 * 1024 * 1024; //Integer.parseInt("1");;//System.getenv("download-bytes");
            new PerfTest2(isServer, serverAddress, transport, uploadBytes, downloadBytes);
        } catch (Throwable t) {
            t.printStackTrace();
            System.err.println("Unexpected exit: " + t);
            System.exit(-1);
        }
    }
}