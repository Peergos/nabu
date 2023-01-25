package org.peergos.protocol.dht;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import org.peergos.protocol.dht.pb.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class KademliaEngine {

    private final Map<PeerId, KademliaController> conns = new ConcurrentHashMap<>();

    public KademliaEngine() {

    }

    public void addConnection(PeerId peer, KademliaController controller) {
        if (conns.containsKey(peer))
            System.out.println("WARNING overwriting peer connection value");
        conns.put(peer, controller);
    }


    private static byte[] prefixBytes(Cid c) {
        ByteArrayOutputStream res = new ByteArrayOutputStream();
        try {
            Cid.putUvarint(res, c.version);
            Cid.putUvarint(res, c.codec.type);
            Cid.putUvarint(res, c.getType().index);
            Cid.putUvarint(res, c.getType().length);;
            return res.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void receiveMessage(Dht.Message msg, PeerId source) {
        KademliaController conn = conns.get(source);
        conn.receive(msg);
    }
}
