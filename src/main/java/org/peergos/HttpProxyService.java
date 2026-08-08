package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.ConnectionClosedException;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.peergos.net.ConnectionException;
import org.peergos.net.ProxyRequest;
import org.peergos.net.ProxyResponse;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.http.HttpProtocol;
import org.peergos.util.Logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

public class HttpProxyService {

    private static final Logger LOG = Logging.LOG();
    private final Host node;
    private final HttpProtocol.Binding p2pHttpBinding;
    private final Kademlia dht;
    public static final String API_URL = "/p2p/";

    public HttpProxyService(Host node, HttpProtocol.Binding p2pHttpBinding, Kademlia dht) {
        this.node = node;
        this.p2pHttpBinding = p2pHttpBinding;
        this.dht = dht;
    }

    public static Multiaddr[] getAddresses(Host node, Kademlia dht, Multihash targetNodeId) throws ConnectionException {
        AddressBook addressBook = node.getAddressBook();
        Multihash targetPeerId = targetNodeId.bareMultihash();
        PeerId peerId = PeerId.fromBase58(targetPeerId.toBase58());
        Collection<Multiaddr> all = addressBook.get(peerId).join();
        if (! all.isEmpty())
            return all.toArray(Multiaddr[]::new);
        return lookupAddresses(node, dht, targetNodeId, true);
    }

    /** Ask the network where a peer is now, replacing whatever we had cached for them. Cached addresses
     *  go stale - a peer moves, or we cached one that was never dialable - and a node whose only cached
     *  addresses are dead would otherwise never reach that peer again.
     *
     * @param useCached false to force a real dht query. The dht answers a single peer lookup from the
     *                  address book when it can, which is no use to a caller escaping a bad entry.
     */
    public static Multiaddr[] lookupAddresses(Host node, Kademlia dht, Multihash targetNodeId, boolean useCached) throws ConnectionException {
        Multihash targetPeerId = targetNodeId.bareMultihash();
        PeerId peerId = PeerId.fromBase58(targetPeerId.toBase58());
        List<PeerAddresses> closestPeers = dht.findClosestPeers(targetPeerId, 1, node, useCached);
        Optional<PeerAddresses> matching = closestPeers.stream().filter(p -> p.peerId.equals(targetPeerId)).findFirst();
        if (matching.isEmpty()) {
            throw new ConnectionException("Target not found: " + targetPeerId);
        }
        Multiaddr[] allAddresses = matching.get().addresses.stream()
                .map(a -> Multiaddr.fromString(a.toString()))
                .toArray(Multiaddr[]::new);
        node.getAddressBook().setAddrs(peerId, 0, allAddresses);
        return allAddresses;
    }

    /** Dial a peer, and if every address we have for them fails, ask the dht where they are now and try
     *  the answer. Without this a single bad entry in the address book takes a peer out permanently: the
     *  book is non empty, so we never look them up again, and every address in it is dead. */
    private HttpProtocol.HttpController dial(PeerId peerId,
                                             Multihash targetNodeId,
                                             Multiaddr[] addressesToDial) throws ConnectionException {
        try {
            return p2pHttpBinding.dial(node, peerId, addressesToDial).getController().join();
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof ConnectionClosedException)
                return p2pHttpBinding.dial(node, peerId, addressesToDial).getController().join();
            // libp2p's anyComplete only reports the error of whichever dial finished last and drops the
            // rest, so the exception names one arbitrary address out of the set. Log what we actually
            // tried, otherwise there is no way to tell a peer with one dead address from one with thirty.
            LOG.info("Failed to dial " + targetNodeId + " at all of " + Arrays.toString(addressesToDial));
            Multiaddr[] fresh;
            try {
                fresh = lookupAddresses(node, dht, targetNodeId, false);
            } catch (Exception lookupFailed) {
                throw e; // the dial failure is the more useful error
            }
            if (Set.copyOf(Arrays.asList(fresh)).equals(Set.copyOf(Arrays.asList(addressesToDial))))
                throw e; // the dht has nothing new for us, so a retry would fail the same way
            LOG.info("Re-resolved " + targetNodeId + " to " + Arrays.toString(fresh));
            return p2pHttpBinding.dial(node, peerId, fresh).getController().join();
        }
    }

    public ProxyResponse proxyRequest(Multihash targetNodeId, ProxyRequest request) throws IOException, ConnectionException {
        Multiaddr[] addressesToDial = getAddresses(node, dht, targetNodeId);
        PeerId peerId = PeerId.fromBase58(targetNodeId.bareMultihash().toBase58());
        HttpProtocol.HttpController proxier = dial(peerId, targetNodeId, addressesToDial);
        String urlParams = constructQueryParamString(request.queryParams);
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.valueOf(request.method.name()),
                request.path + urlParams, request.body != null ?
                Unpooled.wrappedBuffer(request.body) : Unpooled.buffer(0));

        HttpHeaders reqHeaders = httpRequest.headers();
        for(Map.Entry<String, List<String>> entry : request.headers.entrySet()) {
            reqHeaders.set(entry.getKey(), entry.getValue());
        }
        reqHeaders.set(HttpHeaderNames.CONTENT_LENGTH, request.body != null ? request.body.length : 0);
        FullHttpResponse resp = proxier.send(httpRequest.retain()).join();
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            int contentLength = resp.headers().getInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            resp.content().readBytes(bout, contentLength);
            Map<String, String> headers = new HashMap<>();
            for (Map.Entry<String, String> entry: resp.headers().entries()) {
                String key = entry.getKey();
                if (key != null) {
                    headers.put(key, entry.getValue());
                }
            }
            int code = resp.status().code();
            return new ProxyResponse(bout.toByteArray(), headers, code);
        } finally {
            resp.release();
        }
    }
    private String constructQueryParamString(Map<String, List<String>> queryParams) {
        StringBuilder sb = new StringBuilder();
        if (!queryParams.isEmpty()) {
            sb.append("?");
            for (Map.Entry<String, List<String>> entry: queryParams.entrySet()) {
                for(String value : entry.getValue()) {
                    sb.append(entry.getKey() + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8) + "&");
                }
            }
        }
        return sb.toString();
    }
}
