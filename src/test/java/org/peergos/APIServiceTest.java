package org.peergos;

import io.ipfs.cid.Cid;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.peergos.blockstore.Blockstore;
import org.peergos.blockstore.FileBlockstore;
import org.peergos.blockstore.RamBlockstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Optional;

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
    public class Tester {
        public static void runAPIServiceTest(Blockstore blocks) {
            APIService service = new APIService(blocks);
            Cid cid = Cid.decode("zdpuAwfJrGYtiGFDcSV3rDpaUrqCtQZRxMjdC6Eq9PNqLqTGg");
            Assert.assertFalse("cid found", service.hasBlock(cid).join());
            String text = "Hello world!";
            byte[] block = text.getBytes();

            Cid cidAdded = service.putBlock(block, "raw").join();
            Assert.assertTrue("cid added was found", service.hasBlock(cidAdded).join());

            Optional<byte[]> blockRetrieved = service.getBlock(cidAdded, false).join();
            Assert.assertTrue("block retrieved", blockRetrieved.isPresent());
            Assert.assertTrue("block is as expected", text.equals(new String(blockRetrieved.get())));

            Assert.assertTrue("block removed", service.rmBlock(cidAdded).join());
            Assert.assertFalse("cid still found", service.hasBlock(cidAdded).join());

        }
    }
}
