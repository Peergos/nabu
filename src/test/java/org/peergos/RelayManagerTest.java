package org.peergos;

import io.ipfs.multihash.Multihash;
import io.libp2p.core.crypto.*;
import io.libp2p.crypto.keys.*;
import org.junit.*;
import org.peergos.protocol.circuit.*;

import java.util.*;
import java.util.concurrent.atomic.*;

public class RelayManagerTest {

    private static Multihash peer(int i) {
        byte[] raw = new byte[32];
        raw[0] = (byte) i;
        return new Multihash(Multihash.Type.sha2_256, raw);
    }

    @Test
    public void refusesReservationsWhenNotReachable() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        Multihash relayId = peer(0);
        AtomicBoolean reachable = new AtomicBoolean(false);
        CircuitHopProtocol.RelayManager manager =
                CircuitHopProtocol.RelayManager.limitTo(priv, relayId, 5, reachable::get);

        Assert.assertTrue("no reservation while not reachable",
                manager.createReservation(peer(1)).isEmpty());

        reachable.set(true);
        Assert.assertTrue("reservation granted once reachable",
                manager.createReservation(peer(1)).isPresent());
    }

    @Test
    public void honoursConcurrentLimit() {
        PrivKey priv = Ed25519Kt.generateEd25519KeyPair().getFirst();
        CircuitHopProtocol.RelayManager manager =
                CircuitHopProtocol.RelayManager.limitTo(priv, peer(0), 2, () -> true);

        Assert.assertTrue(manager.createReservation(peer(1)).isPresent());
        Assert.assertTrue(manager.createReservation(peer(2)).isPresent());
        Assert.assertTrue("third reservation exceeds the limit",
                manager.createReservation(peer(3)).isEmpty());
    }
}
