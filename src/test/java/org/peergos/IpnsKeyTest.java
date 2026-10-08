package org.peergos;

import com.google.protobuf.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.crypto.*;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.protocol.ipns.*;

import java.time.*;
import java.util.*;

/** Regression tests for IPNS routing keys.
 *
 *  Standard IPNS names for inlined Ed25519 keys are identity multihashes. These used to be
 *  parsed with Cid.cast on the GET_VALUE path, which threw CidEncodingException and killed the
 *  kademlia handler thread, while the PUT_VALUE path stored records under Multihash.deserialize.
 *  getPublisherFromKey must produce the same key as the store side for identity multihashes.
 *
 *  Records under an RSA name carry their own public key, which used to be trusted without checking
 *  that it hashes to the name, so anyone could sign a record that validated under any RSA name.
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

    @Test
    public void acceptsRecordsSignedByTheNamedKey() {
        PrivKey rsa = RsaKt.generateRsaKeyPair(2048).getFirst();
        PrivKey ed25519 = Ed25519Kt.generateEd25519KeyPair().getFirst();
        Assert.assertTrue(validatesUnder(rsa, record(rsa)));
        Assert.assertTrue(validatesUnder(ed25519, record(ed25519)));
    }

    @Test
    public void rejectsRecordsSignedByAnotherKey() {
        PrivKey rsa = RsaKt.generateRsaKeyPair(2048).getFirst();
        PrivKey ed25519 = Ed25519Kt.generateEd25519KeyPair().getFirst();
        Assert.assertFalse(validatesUnder(rsa, record(RsaKt.generateRsaKeyPair(2048).getFirst())));
        Assert.assertFalse(validatesUnder(ed25519, record(Ed25519Kt.generateEd25519KeyPair().getFirst())));
    }

    private static byte[] record(PrivKey signer) {
        return IPNS.createSignedRecord("/ipfs/bafkqaaa".getBytes(), LocalDateTime.now().plusHours(1), 1,
                3600_000_000_000L, Optional.empty(), Optional.empty(), signer);
    }

    private static boolean validatesUnder(PrivKey nameKey, byte[] record) {
        Multihash name = Multihash.deserialize(PeerId.fromPubKey(nameKey.publicKey()).getBytes());
        return IPNS.parseAndValidateIpnsEntry(IPNS.getKey(name), record).isPresent();
    }
}
