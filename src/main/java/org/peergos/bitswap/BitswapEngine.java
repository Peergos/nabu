package org.peergos.bitswap;

import bitswap.message.pb.*;
import com.google.protobuf.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import org.peergos.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.stream.*;

public class BitswapEngine {

    private final Blockstore store;
    private final ConcurrentHashMap<Cid, CompletableFuture<byte[]>> localWants = new ConcurrentHashMap<>();
    private final Map<PeerId, BitswapController> conns = new ConcurrentHashMap<>();

    public BitswapEngine(Blockstore store) {
        this.store = store;
    }

    public void addConnection(PeerId peer, BitswapController controller) {
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

    public void receiveMessage(MessageOuterClass.Message msg, PeerId source) {
        List<MessageOuterClass.Message.BlockPresence> presences = new ArrayList<>();
        List<MessageOuterClass.Message.Block> blocks = new ArrayList<>();
        List<MessageOuterClass.Message.Wantlist.Entry> wants = new ArrayList<>();
        if (msg.hasWantlist()) {
            for (MessageOuterClass.Message.Wantlist.Entry e : msg.getWantlist().getEntriesList()) {
                Cid c = Cid.cast(e.getBlock().toByteArray());
                boolean isCancel = e.getCancel();
                boolean sendDontHave = e.getSendDontHave();
                boolean wantBlock = e.getWantType().getNumber() == 0;
                if (wantBlock) {
                    Optional<byte[]> block = store.get(c).join();
                    // TODO split into multiple messages if response is too big
                    if (block.isPresent())
                        blocks.add(MessageOuterClass.Message.Block.newBuilder()
                                        .setPrefix(ByteString.copyFrom(prefixBytes(c)))
                                        .setData(ByteString.copyFrom(block.get()))
                                .build());
                    else if (sendDontHave)
                        presences.add(MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build());
                } else {
                    boolean hasBlock = store.has(c).join();
                    if (hasBlock)
                        presences.add(MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.Have)
                                .build());
                    else if (sendDontHave)
                        presences.add(MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build());
                }
            }
        }

        for (MessageOuterClass.Message.Block block : msg.getPayloadList()) {
            byte[] cidPrefix = block.getPrefix().toByteArray();
            byte[] data = block.getData().toByteArray();
            ByteArrayInputStream bin = new ByteArrayInputStream(cidPrefix);
            try {
                long version = Cid.readVarint(bin);
                Cid.Codec codec = Cid.Codec.lookup(Cid.readVarint(bin));
                Multihash.Type type = Multihash.Type.lookup((int)Cid.readVarint(bin));
//                int hashSize = (int)Cid.readVarint(bin);
                if (type != Multihash.Type.sha2_256) {
                    Logger.getGlobal().info("Unsupported hash algorithm " + type.name());
                } else {
                    byte[] hash = Hash.sha256(data);
                    Cid c = new Cid(version, codec, type, hash);
                    CompletableFuture<byte[]> waiter = localWants.get(c);
                    if (waiter != null) {
                        store.put(data, codec);
                        waiter.complete(data);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        for (MessageOuterClass.Message.BlockPresence blockPresence : msg.getBlockPresencesList()) {
            Cid c = Cid.cast(blockPresence.getCid().toByteArray());
            boolean have = blockPresence.getType().getNumber() == 0;
            if (have && localWants.containsKey(c)) {
                wants.add(MessageOuterClass.Message.Wantlist.Entry.newBuilder()
                        .setBlock(ByteString.copyFrom(c.toBytes()))
                        .setWantType(MessageOuterClass.Message.Wantlist.WantType.Block)
                        .build());
            }
        }

        if (presences.isEmpty() && blocks.isEmpty() && wants.isEmpty())
            return;
        BitswapController conn = conns.get(source);
        if (conn != null) {
            MessageOuterClass.Message.Builder builder = MessageOuterClass.Message.newBuilder()
                    .setWantlist(MessageOuterClass.Message.Wantlist.newBuilder()
                            .addAllEntries(wants).build())
                    .addAllBlockPresences(presences)
                    .addAllPayload(blocks);
            conn.send(builder.build());
        }
        else Logger.getGlobal().info("No connection to send reply bitswap message on!");
    }

    public CompletableFuture<byte[]> get(Cid hash) {
        CompletableFuture<byte[]> res = localWants.get(hash);
        if (res != null)
            return res;
        res = new CompletableFuture<>();
        localWants.put(hash, res);
        sendWantHaves();
        return res;
    }

    public void sendWantHaves() {
        List<MessageOuterClass.Message.Wantlist.Entry> wants = localWants.keySet().stream()
                .map(cid -> MessageOuterClass.Message.Wantlist.Entry.newBuilder()
                        .setWantType(MessageOuterClass.Message.Wantlist.WantType.Have)
                        .setBlock(ByteString.copyFrom(cid.toBytes()))
                        .build())
                .collect(Collectors.toList());
        MessageOuterClass.Message msg = MessageOuterClass.Message.newBuilder()
                .setWantlist(MessageOuterClass.Message.Wantlist.newBuilder().addAllEntries(wants).build()).build();
        conns.forEach((p, conn) -> conn.send(msg));
    }
}
