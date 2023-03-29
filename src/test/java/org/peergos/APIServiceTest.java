package org.peergos;

import io.ipfs.cid.Cid;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.peergos.blockstore.FileBlockstore;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.RamBlockstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public class APIServiceTest {

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
    public void runAPIServiceWithRAMStorageTest() {
        Tester.runAPIServiceTest(new RamBlockstore());
    }
    @Test
    public void runAPIServiceWithFileStorageTest() {
        FileBlockstore blocks = new FileBlockstore(TMP_DATA_FOLDER.toPath());
        Tester.runAPIServiceTest(blocks);
    }

    @Test
    public void bulkGetTest() {
        APIService service = new APIService(new RamBlockstore(), new BitswapBlockService(null, null));
        Cid cid1 = service.putBlock("Hello".getBytes(), "raw");
        Cid cid2= service.putBlock("world!".getBytes(), "raw");
        List<Want> wants = new ArrayList<>();
        wants.add(new Want(cid1, Optional.of("auth")));
        wants.add(new Want(cid2, Optional.of("auth")));
        List<HashedBlock> blocks = service.getBlocks(wants, Collections.emptySet(), false);
        Assert.assertTrue("blocks retrieved", blocks.size() == 2);
    }

    public class Tester {
        public static void runAPIServiceTest(Blockstore blocks) {
            APIService service = new APIService(blocks, new BitswapBlockService(null, null));
            Cid cid = Cid.decode("zdpuAwfJrGYtiGFDcSV3rDpaUrqCtQZRxMjdC6Eq9PNqLqTGg");
            Assert.assertFalse("cid found", service.hasBlock(cid));
            String text = "Hello world!";
            byte[] block = text.getBytes();

            Cid cidAdded = service.putBlock(block, "raw");
            Assert.assertTrue("cid added was found", service.hasBlock(cidAdded));

            List<HashedBlock> blockRetrieved = service.getBlocks(List.of(new Want(cidAdded)), Collections.emptySet(), false);
            Assert.assertTrue("block retrieved", blockRetrieved.size() == 1);
            Assert.assertTrue("block is as expected", text.equals(new String(blockRetrieved.get(0).block)));

            List<Cid> localRefs = service.getRefs();
            for (Cid ref : localRefs) {
                List<HashedBlock> res = service.getBlocks(List.of(new Want(ref)), Collections.emptySet(), false);
                Assert.assertTrue("ref retrieved", res.size() == 1);
            }

            Assert.assertTrue("block removed", service.rmBlock(cidAdded));
            Assert.assertFalse("cid still found", service.hasBlock(cidAdded));

        }
    }
}
