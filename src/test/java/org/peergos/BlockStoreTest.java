package org.peergos;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.peergos.blockstore.FileBlockstore;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;

public class BlockStoreTest {

    private final static File TMP_DATA_FOLDER = new File("temp-blockstore");

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
    }

    @Test
    public void confirmShardFunc() {
        //from description in ./ipfs/blocks/_README
        FileBlockstore bs = new FileBlockstore(TMP_DATA_FOLDER.toPath());
        Cid cid  = Cid.decode("zb2rhYSxw4ZjuzgCnWSt19Q94ERaeFhu9uSqRgjSdx9bsgM6f");
        Path path = bs.getFilePath(cid);
        String expected = "blocks/" + "SC/AFKREIA22FLID5AJ2KU7URG47MDLROZIH6YF2KALU2PWEFPVI37YLKRSCA.data";
        Assert.assertTrue("ShardFunc does not match /repo/flatfs/shard/v1/next-to-last/2", path.toString().equals(expected));
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

}
