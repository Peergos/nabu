package org.peergos;

import com.offbynull.kademlia.*;
import org.junit.*;

import java.time.*;
import java.util.*;

import static io.libp2p.etc.types.ByteArrayExtKt.toHex;

public class RouterTest {

    @Test
    public void nearestNodes() {
        Router r = new Router(Id.create(new byte[32], 256), 2, 2, 2);
        byte[] key = new byte[32];
        Random rnd = new Random(1);
        for (int i=0; i < 100; i++) {
            rnd.nextBytes(key);
            r.touch(Instant.now(), new Node(Id.create(key, 256), toHex(key)));
        }
        List<Node> nodesNearZero = r.find(Id.create(new byte[32], 256), 20, false);
        Assert.assertTrue(nodesNearZero.size() > 10);
        byte[] randomKey = new byte[32];
        rnd.nextBytes(randomKey);
        List<Node> nodesNearRandom = r.find(Id.create(randomKey, 256), 20, false);
        Assert.assertTrue(nodesNearRandom.size() > 10);
    }
}
