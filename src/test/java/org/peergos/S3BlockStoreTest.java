package org.peergos;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.junit.*;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;
import org.peergos.blockstore.metadatadb.JdbcBlockMetadataStore;
import org.peergos.blockstore.metadatadb.sql.H2BlockMetadataCommands;
import org.peergos.blockstore.metadatadb.sql.UncloseableConnection;
import org.peergos.blockstore.s3.S3Blockstore;

import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class S3BlockStoreTest {

    private static final String BUCKET = "test-bucket";
    private static final String ACCESS_KEY = "testkey";
    private static final String SECRET_KEY = "testsecret";
    private static final int PORT = 9002;

    private static LocalS3Server s3Server;
    private static Path storageRoot;

    @BeforeClass
    public static void startServer() throws Exception {
        storageRoot = Files.createTempDirectory("nabu-s3-test");
        s3Server = new LocalS3Server(storageRoot, BUCKET, ACCESS_KEY, SECRET_KEY, PORT);
        s3Server.start();
    }

    @AfterClass
    public static void stopServer() {
        if (s3Server != null)
            s3Server.stop();
    }

    @Test
    public void testS3FileStoreWithRamMetaDataStore() {
        BlockMetadataStore metadata = new RamBlockMetadataStore();
        testFileStore(metadata);
    }

    @Test
    public void testS3FileStoreWithJDBCMetaDataStore() throws Exception {
        Connection h2Instance = DriverManager.getConnection("jdbc:h2:" +
            "mem:" + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH");
        Connection instance = new UncloseableConnection(h2Instance);
        instance.setAutoCommit(true);
        BlockMetadataStore metadata = new JdbcBlockMetadataStore(() -> instance, new H2BlockMetadataCommands());
        testFileStore(metadata);
    }

    public void testFileStore(BlockMetadataStore metadata) {
        Map<String, Object> params = LocalS3Server.getParams(BUCKET, ACCESS_KEY, SECRET_KEY, PORT);
        S3Blockstore bs = new S3Blockstore(params, metadata);

        String msg = "hello world!";
        byte[] block = msg.getBytes();
        Cid.Codec codec = Cid.Codec.Raw;
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));

        bs.rm(cid).join();

        boolean found = bs.has(cid).join();
        Assert.assertTrue("Found cid", !found);

        Cid newCid = bs.put(block, codec).join();
        Assert.assertTrue("cid match", cid.equals(newCid));
        newCid = bs.put(block, codec).join();

        found = bs.has(cid).join();
        Assert.assertTrue("NOT Found cid", found);

        Optional<byte[]> data = bs.get(cid).join();
        Assert.assertTrue("data empty", !data.isEmpty());
        String str = new String(data.get());
        Assert.assertTrue("data match", str.equals(msg));

        bs.rm(cid).join();
        found = bs.has(cid).join();
        Assert.assertTrue("Found cid", !found);
    }
}
