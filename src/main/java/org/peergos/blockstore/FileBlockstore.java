package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class FileBlockstore implements Blockstore {

    private static final Logger LOG = Logger.getLogger(FileBlockstore.class.getName());

    private final Path root;
    private final int folderDepth = 5;

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

    private static String hashToKey(Multihash hash) {
        String padded = new Base32().encodeAsString(hash.toBytes());
        int padStart = padded.indexOf("=");
        return padStart > 0 ? padded.substring(0, padStart) : padded;
    }

    private Path getFilePath(Cid cid) {
        String name = hashToKey(cid);
        int folderPrefixLength = 2;
        Path path = Paths.get(name.substring(0, folderPrefixLength));
        for (int i = 0; i < folderDepth; i++)
            path = path.resolve(Character.toString(name.charAt(folderPrefixLength + i)));
        path = path.resolve(name);
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
}
