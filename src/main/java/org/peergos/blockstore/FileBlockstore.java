package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;
import org.peergos.blockstore.metadatadb.BlockMetadata;
import org.peergos.util.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileBlockstore implements Blockstore {

    private static final Logger LOG = Logging.LOG();

    private final Path blocksRoot;
    private final String BLOCKS = "blocks";
    private final String BLOCK_FILE_SUFFIX = ".data";

    public FileBlockstore(Path root) {
        if (root == null || !root.toFile().isDirectory()) {
            throw new IllegalStateException("Path must be a directory! " + root);
        }
        Path blocksPath = root.resolve(BLOCKS);
        File blocksDirectory = blocksPath.toFile();
        if (!blocksDirectory.exists()) {
            if (!blocksDirectory.mkdirs()) {
                throw new IllegalStateException("Unable to make blocks directory");
            }
        } else if (blocksDirectory.isFile()) {
            throw new IllegalStateException("Unable to create blocks directory");
        }
        this.blocksRoot = blocksPath;
        LOG.info("Using FileBlockStore at location: " + blocksPath);
    }

    public Path getFilePath(Cid cid) {
        String key = hashToKey(cid);
        String folder = key.substring(key.length() -3, key.length()-1);
        String filename = key + BLOCK_FILE_SUFFIX;

        Path path = Paths.get(folder);
        path = path.resolve(filename);
        return path;
    }

    @Override
    public CompletableFuture<Boolean> has(Cid cid) {
        Path path = getFilePath(cid);
        File file = blocksRoot.resolve(path).toFile();
        return CompletableFuture.completedFuture(file.exists());
    }

    @Override
    public CompletableFuture<Boolean> hasAny(Multihash h) {
        return Futures.of(Stream.of(Cid.Codec.DagCbor, Cid.Codec.Raw, Cid.Codec.DagProtobuf)
                .anyMatch(c -> has(new Cid(1, c, h.getType(), h.getHash())).join()));
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid cid) {
        try {
            Path path = getFilePath(cid);
            File file = blocksRoot.resolve(path).toFile();
            if (!file.exists()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            try (DataInputStream din = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
                byte[] buffer = new byte[1024];
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                for (int len; (len = din.read(buffer)) != -1; ) {
                    bout.write(buffer, 0, len);
                }
                return CompletableFuture.completedFuture(Optional.of(bout.toByteArray()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));
        try {
            Path filePath = getFilePath(cid);
            Path target = blocksRoot.resolve(filePath);
            Path parent = target.getParent();
            File parentDir = parent.toFile();

            if (!parentDir.exists())
                Files.createDirectories(parent);

            for (Path someParent = parent; !someParent.equals(blocksRoot); someParent = someParent.getParent()) {
                File someParentFile = someParent.toFile();
                if (!someParentFile.canWrite()) {
                    final boolean b = someParentFile.setWritable(true, false);
                    if (!b)
                        throw new IllegalStateException("Could not make " + someParent.toString() + ", ancestor of " + parentDir.toString() + " writable");
                }
            }
            Files.write(target, block, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            return CompletableFuture.completedFuture(cid);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Boolean> rm(Cid cid) {
        Path path = getFilePath(cid);
        File file = blocksRoot.resolve(path).toFile();
        if (file.exists()) {
            return CompletableFuture.completedFuture(file.delete());
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public CompletableFuture<Boolean> bloomAdd(Cid cid) {
        //not implemented
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<List<Cid>> refs(boolean useBlockstore) {
        List<Path> result = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(blocksRoot)) {
            result = walk.filter(f -> Files.isRegularFile(f) &&
                            f.toFile().length() > 0 &&
                            f.getFileName().toString().endsWith(BLOCK_FILE_SUFFIX))
                    .collect(Collectors.toList());
        } catch (IOException ioe) {
            LOG.log(Level.WARNING, "Unable to retrieve local refs: " + ioe);
        }
        List<Cid> cidList = result.stream().map(p -> {
            String filename = p.getFileName().toString();
            return keyToHash(filename.substring(0, filename.length() - BLOCK_FILE_SUFFIX.length()));
        }).collect(Collectors.toList());
        return CompletableFuture.completedFuture(cidList);
    }

    @Override
    public CompletableFuture<Long> count(boolean useBlockstore) {
        try {
            return Futures.of(Files.walk(blocksRoot)
                    .filter(f -> Files.isRegularFile(f) &&
                            f.toFile().length() > 0 &&
                            f.getFileName().toString().endsWith(BLOCK_FILE_SUFFIX))
                    .count());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> applyToAll(Consumer<Cid> action, boolean useBlockstore) {
        try {
            Files.walk(blocksRoot)
                    .filter(f -> Files.isRegularFile(f) &&
                            f.toFile().length() > 0 &&
                            f.getFileName().toString().endsWith(BLOCK_FILE_SUFFIX))
                    .map(p -> {
                        String filename = p.getFileName().toString();
                        return keyToHash(filename.substring(0, filename.length() - BLOCK_FILE_SUFFIX.length()));
                    }).forEach(action);
            return Futures.of(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<BlockMetadata> getBlockMetadata(Cid h) {
        throw new IllegalStateException("Unsupported operation!");
    }
}
