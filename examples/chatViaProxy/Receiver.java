package chatViaProxy;

import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;
import org.peergos.HostBuilder;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.http.HttpProtocol;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class Receiver {
    public Receiver() {
        InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", 8000);
        HostBuilder builder2 = HostBuilder.build(10000 + new Random().nextInt(50000),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(new HttpProtocol.Binding(proxyTarget));
        Host node2 = builder2.build();
        node2.start().join();
        Multiaddr address2 = node2.listenAddresses().get(0);
        System.out.println("Running Multiaddr: " + address2.toString());
        Thread shutdownHook = new Thread(() -> {
            try {
                node2.stop();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
    public static void main(String[] args) {
        new Receiver();
    }
}
