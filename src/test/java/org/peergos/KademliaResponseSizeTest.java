package org.peergos;

import com.google.protobuf.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import org.junit.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.dht.pb.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

/** A DHT query is tiny, and the reply we send back is built from whatever the address book holds for
 *  the 20 closest peers. Address books accumulate addresses for a peer and never shed them, so on a
 *  long lived node the reply grows without bound while the query stays the same size, and we hand out
 *  a large multiple of the bandwidth we are sent. These tests pin the reply to a sane size.
 */
public class KademliaResponseSizeTest {

    private static final int PEERS = 25;
    private static final int ADDRESSES_PER_PEER = 500;
    /** 20 peers of a handful of addresses each is a few KB. Anything near the ~100KB an unbounded
     *  reply reaches is the amplification we are guarding against. */
    private static final int MAX_REPLY_BYTES = 16 * 1024;

    @Test
    public void findNodeReplyIsNotAmplified() {
        KademliaEngine engine = engineWithBloatedAddressBook();
        byte[] target = new byte[32];
        new Random(42).nextBytes(target);

        Dht.Message query = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(target))
                .build();
        Dht.Message reply = send(engine, query);

        Assert.assertTrue("reply should carry the closest peers we know", reply.getCloserPeersCount() > 0);
        int amplification = reply.getSerializedSize() / query.getSerializedSize();
        Assert.assertTrue("FIND_NODE reply was " + reply.getSerializedSize() + " bytes for a "
                        + query.getSerializedSize() + " byte query (" + amplification + "x)",
                reply.getSerializedSize() < MAX_REPLY_BYTES);
    }

    @Test
    public void getProvidersReplyIsNotAmplified() {
        KademliaEngine engine = engineWithBloatedAddressBook();
        Multihash wanted = randomId();

        Dht.Message query = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(wanted.toBytes()))
                .build();
        Dht.Message reply = send(engine, query);

        Assert.assertTrue("GET_PROVIDERS reply was " + reply.getSerializedSize() + " bytes",
                reply.getSerializedSize() < MAX_REPLY_BYTES);
    }

    /** An engine whose address book has accumulated far more addresses per peer than any peer
     *  legitimately announces, which is the state a long running node drifts into. */
    private static KademliaEngine engineWithBloatedAddressBook() {
        KademliaEngine engine = new KademliaEngine(randomId(), new RamProviderStore(1000),
                new RamRecordStore(), Optional.empty());
        RamAddressBook addressBook = new RamAddressBook();
        engine.setAddressBook(addressBook);

        for (int i = 0; i < PEERS; i++) {
            PeerId peer = randomPeerId();
            Multiaddr[] addrs = IntStream.range(0, ADDRESSES_PER_PEER)
                    .mapToObj(j -> new Multiaddr("/ip4/8." + (j / 256) + "." + (j % 256) + ".7/tcp/4001"))
                    .toArray(Multiaddr[]::new);
            addressBook.addAddrs(peer, 0, addrs).join();
            engine.addOutgoingConnection(peer);
        }
        return engine;
    }

    /** A peer id is a multihash of the peer's public key, so random bytes will not deserialize. */
    private static Multihash randomId() {
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        return new Multihash(Multihash.Type.sha2_256, hash);
    }

    private static PeerId randomPeerId() {
        return new PeerId(randomId().toBytes());
    }

    /** Drives the real responder path. The engine only writes to and closes the stream, so a proxy
     *  that captures the written message is enough to see what we would put on the wire. */
    private static Dht.Message send(KademliaEngine engine, Dht.Message query) {
        AtomicReference<Dht.Message> written = new AtomicReference<>();
        Stream stream = (Stream) Proxy.newProxyInstance(
                KademliaResponseSizeTest.class.getClassLoader(),
                new Class[]{Stream.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("writeAndFlush"))
                        written.set((Dht.Message) args[0]);
                    return null;
                });
        engine.receiveRequest(query, randomPeerId(), stream);
        Dht.Message reply = written.get();
        Assert.assertNotNull("engine sent no reply", reply);
        return reply;
    }
}
