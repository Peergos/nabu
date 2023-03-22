package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TypeLimitedBlockstore implements Blockstore {

    private static final Logger LOG = Logger.getLogger(TypeLimitedBlockstore.class.getName());

    private final Blockstore blocks;
    private final Set<Cid.Codec> allowedCodecs;

    public TypeLimitedBlockstore(Blockstore blocks, Set<Cid.Codec> allowedCodecs) {
        this.blocks = blocks;
        this.allowedCodecs = allowedCodecs;
    }

    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.bloomAdd(cid);
        }
        LOG.log(Level.WARNING, "Failed attempt to call bloomAdd with codec: " + cid.codec);
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> has(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.has(cid);
        }
        LOG.log(Level.WARNING, "Failed attempt to call has with codec: " + cid.codec);
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.get(cid);
        }
        LOG.log(Level.WARNING, "Failed attempt to call get with codec: " + cid.codec);
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        if (allowedCodecs.contains(codec)) {
            return blocks.put(block, codec);
        }
        LOG.log(Level.WARNING, "Failed attempt to call put with codec: " + codec);
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));
        return CompletableFuture.completedFuture(cid);
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid cid) {
        if (allowedCodecs.contains(cid.codec)) {
            return blocks.rm(cid);
        }
        LOG.log(Level.WARNING, "Failed attempt to call rm with codec: " + cid.codec);
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<List<Cid>> refs() {
        return blocks.refs();
    }
}
