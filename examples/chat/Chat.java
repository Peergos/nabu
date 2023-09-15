package chat;

import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.peergos.BlockRequestAuthoriser;
import org.peergos.EmbeddedIpfs;
import org.peergos.HostBuilder;
import org.peergos.PeerAddresses;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.config.Config;
import org.peergos.config.IdentitySection;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.dht.RecordStore;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.Version;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class Chat {

    private EmbeddedIpfs embeddedIpfs;

    public static final Version CURRENT_VERSION = Version.parse("0.0.1");

    private static final Logger LOG = Logger.getGlobal();



    private static HttpProtocol.HttpRequestProcessor proxyHandler() {
        return (s, req, h) -> {
            ByteBuf content = ((FullHttpRequest) req).content();
            CharSequence contents = content.getCharSequence(0, content.readableBytes(), Charset.defaultCharset());
            String output = contents.toString();
            System.out.println("received msg:" + output);
            FullHttpResponse replyOk = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.buffer(0));
            replyOk.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            h.accept(replyOk.retain());
        };
    }

    public Chat() {
        RecordStore recordStore = new RamRecordStore();
        Blockstore blockStore = new RamBlockstore();

        LOG.info("Starting Chat version: " + CURRENT_VERSION);
        int portNumber = 10000 + new Random().nextInt(50000);
        List<MultiAddress> swarmAddresses = List.of(new MultiAddress("/ip6/::/tcp/" + portNumber));
        List<MultiAddress> bootstrapNodes = new ArrayList<>(Config.defaultBootstrapNodes);

        HostBuilder builder = new HostBuilder().generateIdentity();
        PrivKey privKey = builder.getPrivateKey();
        PeerId peerId = builder.getPeerId();
        System.out.println("My PeerId:" + peerId.toBase58());
        IdentitySection identitySection = new IdentitySection(privKey.bytes(), peerId);
        BlockRequestAuthoriser authoriser = (c, b, p, a) -> CompletableFuture.completedFuture(true);

        embeddedIpfs = EmbeddedIpfs.build(recordStore, blockStore,
                swarmAddresses,
                bootstrapNodes,
                identitySection,
                authoriser, Optional.of(Chat.proxyHandler()));
        embeddedIpfs.start();
        Thread shutdownHook = new Thread(() -> {
            LOG.info("Stopping server...");
            try {
                embeddedIpfs.stop().join();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        System.out.println("Enter PeerId of other node:");
        Scanner in = new Scanner(System.in);
        String peerIdStr = in.nextLine().trim();
        if (peerIdStr.length() == 0) {
            throw new IllegalArgumentException("Invalid PeerId");
        }
        Multihash targetNodeId = Multihash.fromBase58(peerIdStr);

        AddressBook addressBook = embeddedIpfs.node.getAddressBook();
        PeerId targetPeerId = PeerId.fromBase58(targetNodeId.bareMultihash().toBase58());
        Optional<Multiaddr> targetAddressesOpt = addressBook.get(targetPeerId).join().stream().findFirst();
        Multiaddr[] allAddresses = null;
        if (targetAddressesOpt.isEmpty()) {
            List<PeerAddresses> closestPeers = embeddedIpfs.dht.findClosestPeers(targetNodeId, 1, embeddedIpfs.node);
            Optional<PeerAddresses> matching = closestPeers.stream().filter(p -> p.peerId.equals(targetNodeId)).findFirst();
            if (matching.isEmpty()) {
                throw new IllegalStateException("Target not found: " + targetNodeId);
            }
            PeerAddresses peer = matching.get();
            allAddresses = peer.addresses.stream().map(a -> Multiaddr.fromString(a.toString())).toArray(Multiaddr[]::new);
        }
        Multiaddr[] addressesToDial = targetAddressesOpt.isPresent() ?
                Arrays.asList(targetAddressesOpt.get()).toArray(Multiaddr[]::new)
                : allAddresses;

        runChat(embeddedIpfs.node, embeddedIpfs.p2pHttp.get(), targetPeerId, addressesToDial);
    }
    private void runChat(Host node, HttpProtocol.Binding p2pHttpBinding, PeerId targetPeerId, Multiaddr[] addressesToDial) {
        System.out.println("Type message:");
        Scanner in = new Scanner(System.in);
        while (true) {
            byte[] msg = in.nextLine().trim().getBytes();
            FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.copiedBuffer(msg));
            httpRequest.headers().set(HttpHeaderNames.CONTENT_LENGTH, msg.length);
            HttpProtocol.HttpController proxier = p2pHttpBinding.dial(node, targetPeerId, addressesToDial).getController().join();
            proxier.send(httpRequest.retain()).join().release();
        }
    }

    public static void main(String[] args) {
        new Chat();
    }

}
