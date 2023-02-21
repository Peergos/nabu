package org.peergos;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.peergos.blockstore.FileBlockstore;
import org.peergos.blockstore.SQLiteBlockstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Optional;

public class BlockStoreTest {

    private final static File TMP_DATA_FOLDER = new File("temp-blockstore");
    private final static File TMP_DATABASE = new File("temp-database");

    @BeforeClass
    public static void createTempFolder() throws IOException {
        deleteTempFiles();
        TMP_DATA_FOLDER.mkdirs();
    }

    @AfterClass
    public static void deleteTempFiles() throws IOException {
        if (TMP_DATA_FOLDER.exists()) {
            Files.walk(TMP_DATA_FOLDER.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
        }
        if (TMP_DATABASE.exists()) {
            Files.delete(TMP_DATABASE.toPath());
        }
    }

    @Test
    public void testFileStore() {

        FileBlockstore bs = new FileBlockstore(TMP_DATA_FOLDER.toPath());
        String msg = "hello world!";
        byte[] block = msg.getBytes();
        Cid.Codec codec = Cid.Codec.Raw;
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));
        boolean found = bs.has(cid).join();
        Assert.assertTrue("Found cid", !found);

        Cid newCid = bs.put(block, codec).join();
        Assert.assertTrue("cid match", cid.equals(newCid));
        //make sure PUTing a second time succeeds
        newCid = bs.put(block, codec).join();

        found = bs.has(cid).join();
        Assert.assertTrue("NOT Found cid", found);

        Optional<byte[]> data = bs.get(cid).join();
        Assert.assertTrue("data empty", !data.isEmpty());
        String str = new String(data.get());
        Assert.assertTrue("data match", str.equals(msg));
    }

    @Test
    public void testSQLiteStore() {
        SQLiteBlockstore bs = new SQLiteBlockstore(TMP_DATABASE.getName());
        String msg = "hello world!";
        byte[] block = msg.getBytes();
        Cid.Codec codec = Cid.Codec.Raw;
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));
        boolean found = bs.has(cid).join();
        Assert.assertTrue("Found cid", !found);

        Cid newCid = bs.put(block, codec).join();
        Assert.assertTrue("cid match", cid.equals(newCid));

        //make sure PUTing a second time succeeds
        newCid = bs.put(block, codec).join();

        found = bs.has(cid).join();
        Assert.assertTrue("NOT Found cid", found);

        Optional<byte[]> data = bs.get(cid).join();
        Assert.assertTrue("data empty", !data.isEmpty());
        String str = new String(data.get());
        Assert.assertTrue("data match", str.equals(msg));
    }
}
