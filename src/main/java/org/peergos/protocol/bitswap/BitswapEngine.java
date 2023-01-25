package org.peergos.protocol.bitswap;

import com.google.protobuf.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import org.peergos.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.pb.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
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
        BitswapController conn = conns.get(source);

        List<MessageOuterClass.Message.BlockPresence> presences = new ArrayList<>();
        List<MessageOuterClass.Message.Block> blocks = new ArrayList<>();
        List<MessageOuterClass.Message.Wantlist.Entry> wants = new ArrayList<>();

        int messageSize = 0;
        if (msg.hasWantlist()) {
            for (MessageOuterClass.Message.Wantlist.Entry e : msg.getWantlist().getEntriesList()) {
                Cid c = Cid.cast(e.getBlock().toByteArray());
                boolean isCancel = e.getCancel();
                boolean sendDontHave = e.getSendDontHave();
                boolean wantBlock = e.getWantType().getNumber() == 0;
                if (wantBlock) {
                    Optional<byte[]> block = store.get(c).join();
                    if (block.isPresent()) {
                        MessageOuterClass.Message.Block blockP = MessageOuterClass.Message.Block.newBuilder()
                                .setPrefix(ByteString.copyFrom(prefixBytes(c)))
                                .setData(ByteString.copyFrom(block.get()))
                                .build();
                        int blockSize = blockP.getSerializedSize();
                        if (blockSize + messageSize > Bitswap.MAX_MESSAGE_SIZE) {
                            buildAndSendMessages(wants, presences, blocks, conn::send);
                            wants = new ArrayList<>();
                            presences = new ArrayList<>();
                            blocks = new ArrayList<>();
                            messageSize = 0;
                        }
                        messageSize += blockSize;
                        blocks.add(blockP);
                    } else if (sendDontHave) {
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    }
                } else {
                    boolean hasBlock = store.has(c).join();
                    if (hasBlock) {
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.Have)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    } else if (sendDontHave) {
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    }
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

        if (conn != null) {
            buildAndSendMessages(wants, presences, blocks, conn::send);
        }
        else Logger.getGlobal().info("No connection to send reply bitswap message on!");
    }

    public CompletableFuture<byte[]> get(Cid hash) {
        return get(List.of(hash)).get(0);
    }

    public List<CompletableFuture<byte[]>> get(List<Cid> hashes) {
        List<CompletableFuture<byte[]>> results = new ArrayList<>();
        for (Cid hash : hashes) {
            CompletableFuture<byte[]> res = localWants.get(hash);
            if (res != null)
                results.add(res);
            else {
                res = new CompletableFuture<>();
                localWants.put(hash, res);
                results.add(res);
            }
        }
        sendWantHaves();
        return results;
    }

    public void sendWantHaves() {
        List<MessageOuterClass.Message.Wantlist.Entry> wants = localWants.keySet().stream()
                .map(cid -> MessageOuterClass.Message.Wantlist.Entry.newBuilder()
                        .setWantType(MessageOuterClass.Message.Wantlist.WantType.Have)
                        .setBlock(ByteString.copyFrom(cid.toBytes()))
                        .build())
                .collect(Collectors.toList());
        // broadcast to all connected peers
        buildAndSendMessages(wants, Collections.emptyList(), Collections.emptyList(),
                msg -> conns.forEach((p, conn) -> conn.send(msg)));
    }

    public void buildAndSendMessages(List<MessageOuterClass.Message.Wantlist.Entry> wants,
                                     List<MessageOuterClass.Message.BlockPresence> presences,
                                     List<MessageOuterClass.Message.Block> blocks,
                                     Consumer<MessageOuterClass.Message> sender) {
        // make sure we stay within the message size limit
        MessageOuterClass.Message.Builder builder = MessageOuterClass.Message.newBuilder();
        int messageSize = 0;
        for (int i=0; i < wants.size(); i++) {
            MessageOuterClass.Message.Wantlist.Entry want = wants.get(i);
            int wantSize = want.getSerializedSize();
            if (wantSize + messageSize > Bitswap.MAX_MESSAGE_SIZE) {
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += wantSize;
            builder = builder.setWantlist(builder.getWantlist().toBuilder().addEntries(want).build());
        }
        for (int i=0; i < presences.size(); i++) {
            MessageOuterClass.Message.BlockPresence presence = presences.get(i);
            int presenceSize = presence.getSerializedSize();
            if (presenceSize + messageSize > Bitswap.MAX_MESSAGE_SIZE) {
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += presenceSize;
            builder = builder.addBlockPresences(presence);
        }
        for (int i=0; i < blocks.size(); i++) {
            MessageOuterClass.Message.Block block = blocks.get(i);
            int blockSize = block.getSerializedSize();
            if (blockSize + messageSize > Bitswap.MAX_MESSAGE_SIZE) {
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += blockSize;
            builder = builder.addPayload(block);
        }
        if (messageSize > 0)
            sender.accept(builder.build());
    }
}
