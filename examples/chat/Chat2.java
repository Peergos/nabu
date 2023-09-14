package chat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.*;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.config.Config;
import org.peergos.config.IdentitySection;
import org.peergos.net.Handler;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.dht.RecordStore;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.HttpUtil;
import org.peergos.util.Version;

import java.io.Console;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class Chat2 {

    private EmbeddedIpfs embeddedIpfs;

    public static final Version CURRENT_VERSION = Version.parse("0.0.1");

    private static final Logger LOG = Logger.getGlobal();

    private static HttpProtocol.HttpRequestProcessor proxyHandler(MultiAddress target) {
        return (s, req, h) -> HttpProtocol.proxyRequest(req, new InetSocketAddress(target.getHost(), target.getPort()), h);
    }

    public Chat2(int portNumber, Multihash targetAddress) throws IOException {
        RecordStore recordStore = new RamRecordStore();
        Blockstore blockStore = new RamBlockstore();

        LOG.info("Starting Chat version: " + CURRENT_VERSION);

        List<MultiAddress> swarmAddresses = List.of(new MultiAddress("/ip6/::/tcp/" + portNumber));
        List<MultiAddress> bootstrapNodes = new ArrayList<>(Config.defaultBootstrapNodes);

        HostBuilder builder = new HostBuilder().generateIdentity();
        PrivKey privKey = builder.getPrivateKey();
        PeerId peerId = builder.getPeerId();

        IdentitySection identitySection = new IdentitySection(privKey.bytes(), peerId);
        BlockRequestAuthoriser authoriser = (c, b, p, a) -> CompletableFuture.completedFuture(true);

        MultiAddress proxyTargetAddress = new MultiAddress("/ip4/127.0.0.1/tcp/8003");
        embeddedIpfs = EmbeddedIpfs.build(recordStore, blockStore,
                swarmAddresses,
                bootstrapNodes,
                identitySection,
                authoriser, Optional.of(Chat2.proxyHandler(proxyTargetAddress)));
        embeddedIpfs.start();

        int apiPort = 10000 + new Random().nextInt(50000);
        MultiAddress apiAddress = new MultiAddress("/ip4/127.0.0.1/tcp/" + apiPort);
        InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

        LOG.info("Starting RPC API server at " + apiAddress.getHost() + ":" + localAPIAddress.getPort());
        HttpServer apiServer = HttpServer.create(localAPIAddress, 500);

        apiServer.createContext(HttpProxyService.API_URL, new Handler() {
            @Override
            public void handleCallToAPI(HttpExchange httpExchange) {
                String path = httpExchange.getRequestURI().getPath();
                System.out.println("PROXY PATH=" + path);
                try {
                    httpExchange.sendResponseHeaders(200, 0);
                    httpExchange.getResponseBody().flush();
                } catch (Exception ex) {
                    HttpUtil.replyError(httpExchange, ex);
                } finally {
                    httpExchange.close();
                }
            }
        });
        apiServer.setExecutor(Executors.newFixedThreadPool(50));
        apiServer.start();

        Thread shutdownHook = new Thread(() -> {
            LOG.info("Stopping server...");
            try {
                embeddedIpfs.stop().join();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        runChat(embeddedIpfs.node, embeddedIpfs.dht, embeddedIpfs.p2pHttp.get(), targetAddress);
    }
    private void runChat(Host node, Kademlia dht, HttpProtocol.Binding p2pHttpBinding, Multihash targetNodeId) {

        AddressBook addressBook = node.getAddressBook();
        PeerId peerId = PeerId.fromBase58(targetNodeId.bareMultihash().toBase58());
        Optional<Multiaddr> targetAddressesOpt = addressBook.get(peerId).join().stream().findFirst();
        Multiaddr[] allAddresses = null;
        if (targetAddressesOpt.isEmpty()) {
            List<PeerAddresses> closestPeers = dht.findClosestPeers(targetNodeId, 1, node);
            Optional<PeerAddresses> matching = closestPeers.stream().filter(p -> p.peerId.equals(targetNodeId)).findFirst();
            if (matching.isEmpty()) {
                throw new IllegalStateException("Target not found: " + targetNodeId);
            }
            PeerAddresses peer = matching.get();
            allAddresses = peer.getPublicAddresses().stream().map(a -> Multiaddr.fromString(a.toString())).toArray(Multiaddr[]::new);
        }
        Multiaddr[] addressesToDial = targetAddressesOpt.isPresent() ?
                Arrays.asList(targetAddressesOpt.get()).toArray(Multiaddr[]::new)
                : allAddresses;
        byte[] msg = "world!".getBytes();
        HttpProtocol.HttpController proxier = p2pHttpBinding.dial(node, peerId, addressesToDial).getController().join();
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.GET, "/p2p/hello", Unpooled.wrappedBuffer(msg));

        HttpHeaders reqHeaders = httpRequest.headers();
        reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, msg.length);
        FullHttpResponse resp = proxier.send(httpRequest.retain()).join();
        int code = resp.status().code();
        resp.release();
        System.currentTimeMillis();
    }

    public static void main(String[] args) throws IOException {
        Console console = System.console();
        System.out.println("Enter swam port");
        Integer portNumber = Integer.parseInt("8456");//console.readLine().trim());
        System.out.println("Enter PeerId of other node");
        String peerId = "12D3KooWDiy5Ng3TXJV1n5tNr11uTXDeV4EZVgUdcPkeSQFAjpUp";
        Multihash targetNodeId = Multihash.deserialize(PeerId.fromBase58(peerId).getBytes());
        //Multihash targetNodeId = Cid.decode("12D3KooWB3gqneGv4NMeJbqxYscpZGBLoC4TPDS6P3PWXbtq3Ubh");//console.readLine().trim());
        new Chat2(portNumber, targetNodeId);
    }

}
