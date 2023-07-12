package org.peergos.protocol.bitswap;

import com.google.protobuf.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import org.peergos.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.pb.*;
import org.peergos.util.*;

import java.io.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;

public class BitswapEngine {
    private static final Logger LOG = Logger.getLogger(BitswapEngine.class.getName());

    private final Blockstore store;
    private final ConcurrentHashMap<Want, CompletableFuture<HashedBlock>> localWants = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Want, Boolean> persistBlocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Want, PeerId> blockHaves = new ConcurrentHashMap<>();
    private final Set<PeerId> connections = new HashSet<>();
    private final BlockRequestAuthoriser authoriser;
    private AddressBook addressBook;

    public BitswapEngine(Blockstore store, BlockRequestAuthoriser authoriser) {
        this.store = store;
        this.authoriser = authoriser;
    }

    public void setAddressBook(AddressBook addrs) {
        this.addressBook = addrs;
    }

    public synchronized void addConnection(PeerId peer, Multiaddr addr) {
        connections.add(peer);
        addressBook.addAddrs(peer, 0, addr);
    }

    public CompletableFuture<HashedBlock> getWant(Want w, boolean addToBlockstore) {
        CompletableFuture<HashedBlock> existing = localWants.get(w);
        if (existing != null)
            return existing;
        CompletableFuture<HashedBlock> res = new CompletableFuture<>();
        if (addToBlockstore)
            persistBlocks.put(w, true);
        localWants.put(w, res);
        return res;
    }

    public boolean hasWants() {
        return ! localWants.isEmpty();
    }

    public Set<PeerId> getConnected() {
        Set<PeerId> connected = new HashSet<>();
        synchronized (connections) {
            connected.addAll(connections);
        }
        return connected;
    }

    public Set<Want> getWants() {
        return localWants.keySet();
    }

    public Map<Want, PeerId> getHaves() {
        return blockHaves;
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

    public void receiveMessage(MessageOuterClass.Message msg, Stream source) {

        List<MessageOuterClass.Message.BlockPresence> presences = new ArrayList<>();
        List<MessageOuterClass.Message.Block> blocks = new ArrayList<>();
        List<MessageOuterClass.Message.Wantlist.Entry> wants = new ArrayList<>();

        int messageSize = 0;
        Multihash peerM = Multihash.deserialize(source.remotePeerId().getBytes());
        Cid sourcePeerId = new Cid(1, Cid.Codec.Libp2pKey, peerM.getType(), peerM.getHash());
        if (msg.hasWantlist()) {
            for (MessageOuterClass.Message.Wantlist.Entry e : msg.getWantlist().getEntriesList()) {
                Cid c = Cid.cast(e.getBlock().toByteArray());
                Optional<String> auth = e.getAuth().isEmpty() ? Optional.empty() : Optional.of(ArrayOps.bytesToHex(e.getAuth().toByteArray()));
                boolean isCancel = e.getCancel();
                boolean sendDontHave = e.getSendDontHave();
                boolean wantBlock = e.getWantType().getNumber() == 0;
                if (wantBlock) {
                    Optional<byte[]> block = store.get(c).join();
                    if (block.isPresent() && authoriser.allowRead(c, block.get(), sourcePeerId, auth.orElse("")).join()) {
                        MessageOuterClass.Message.Block blockP = MessageOuterClass.Message.Block.newBuilder()
                                .setPrefix(ByteString.copyFrom(prefixBytes(c)))
                                .setAuth(ByteString.copyFrom(auth.orElse("").getBytes(StandardCharsets.UTF_8)))
                                .setData(ByteString.copyFrom(block.get()))
                                .build();
                        int blockSize = blockP.getSerializedSize();
                        if (blockSize + messageSize > Bitswap.MAX_MESSAGE_SIZE) {
                            buildAndSendMessages(wants, presences, blocks, source::writeAndFlush);
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

        LOG.info("Bitswap received " + msg.getWantlist().getEntriesCount() + " wants, " + msg.getPayloadCount() +
                " blocks and " + msg.getBlockPresencesCount() + " presences from " + sourcePeerId);
        for (MessageOuterClass.Message.Block block : msg.getPayloadList()) {
            byte[] cidPrefix = block.getPrefix().toByteArray();
            Optional<String> auth = block.getAuth().isEmpty() ?
                    Optional.empty() :
                    Optional.of(ArrayOps.bytesToHex(block.getAuth().toByteArray()));
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
                    Want w = new Want(c, auth);
                    CompletableFuture<HashedBlock> waiter = localWants.get(w);
                    if (waiter != null) {
                        if (persistBlocks.containsKey(w)) {
                            store.put(data, codec);
                            persistBlocks.remove(w);
                        }
                        waiter.complete(new HashedBlock(c, data));
                        localWants.remove(w);
                    } else
                        LOG.info("Received block we don't want: " + c);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (! localWants.isEmpty())
            LOG.info("Remaining: " + localWants.size());
        for (MessageOuterClass.Message.BlockPresence blockPresence : msg.getBlockPresencesList()) {
            Cid c = Cid.cast(blockPresence.getCid().toByteArray());
            Optional<String> auth = blockPresence.getAuth().isEmpty() ?
                    Optional.empty() :
                    Optional.of(ArrayOps.bytesToHex(blockPresence.getAuth().toByteArray()));
            Want w = new Want(c, auth);
            boolean have = blockPresence.getType().getNumber() == 0;
            if (have && localWants.containsKey(w)) {
                blockHaves.put(w, source.remotePeerId());
            }
        }

        if (presences.isEmpty() && blocks.isEmpty() && wants.isEmpty())
            return;

        buildAndSendMessages(wants, presences, blocks, source::writeAndFlush);
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
