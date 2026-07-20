package org.peergos;

import com.google.protobuf.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.protocol.ipns.*;

/** Regression tests for IPNS routing-key parsing.
 *
 *  Standard IPNS names for inlined Ed25519 keys are identity multihashes. These used to be
 *  parsed with Cid.cast on the GET_VALUE path, which threw CidEncodingException and killed the
 *  kademlia handler thread, while the PUT_VALUE path stored records under Multihash.deserialize.
 *  getPublisherFromKey must produce the same key as the store side for identity multihashes.
 */
public class IpnsKeyTest {

    @Test
    public void ed25519IdentityMultihashRoundTrips() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        Multihash peerId = Multihash.deserialize(PeerId.fromPubKey(priv.publicKey()).getBytes());
        // Ed25519 peer ids are inlined as identity multihashes
        Assert.assertEquals(Multihash.Type.id, peerId.getType());

        byte[] routingKey = IPNS.getKey(peerId);
        Multihash parsed = IPNS.getPublisherFromKey(ByteString.copyFrom(routingKey));

        // The GET_VALUE lookup key must match the key records are stored under (the peer id itself)
        Assert.assertEquals(peerId, parsed);
    }

    @Test(expected = IllegalStateException.class)
    public void rejectsNonIpnsKeySpace() {
        IPNS.getPublisherFromKey(ByteString.copyFromUtf8("/pk/whatever"));
    }
}
