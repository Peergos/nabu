package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileBlockstore implements Blockstore {

    private static final Logger LOG = Logger.getLogger(FileBlockstore.class.getName());

    private final Path root;
    private final String BLOCKS = "blocks";
    private final String BLOCK_FILE_SUFFIX = ".data";

    public FileBlockstore(Path root) {
        this.root = root;
        if (root == null || !root.toFile().isDirectory()) {
            throw new IllegalStateException("Path must be a directory! " + root);
        }
        if (!root.toFile().exists()) {
            boolean mkdirs = root.toFile().mkdirs();
            if (!mkdirs) {
                throw new IllegalStateException("Unable to create directory");
            }
        }
        LOG.info("Using FileBlockStore at location: " + root);
    }

    public Path getFilePath(Cid cid) {
        String key = hashToKey(cid);
        String folder = key.substring(key.length() -3, key.length()-1);
        String filename = key + BLOCK_FILE_SUFFIX;

        Path path = Paths.get(BLOCKS);
        path = path.resolve(folder);
        path = path.resolve(filename);
        return path;
    }

    @Override
    public CompletableFuture<Boolean> has(Cid cid) {
        Path path = getFilePath(cid);
        File file = root.resolve(path).toFile();
        return CompletableFuture.completedFuture(file.exists());
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid cid) {
        try {
            Path path = getFilePath(cid);
            File file = root.resolve(path).toFile();
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
            Path target = root.resolve(filePath);
            Path parent = target.getParent();
            File parentDir = parent.toFile();

            if (!parentDir.exists())
                Files.createDirectories(parent);

            for (Path someParent = parent; !someParent.equals(root); someParent = someParent.getParent()) {
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
        File file = root.resolve(path).toFile();
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
    public CompletableFuture<List<Cid>> refs() {
        List<Path> result = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root)) {
            result = walk.filter(Files::isRegularFile)
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
}
