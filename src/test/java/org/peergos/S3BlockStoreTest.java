package org.peergos;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.junit.*;
import org.peergos.blockstore.metadatadb.BlockMetadataStore;
import org.peergos.blockstore.metadatadb.JdbcBlockMetadataStore;
import org.peergos.blockstore.metadatadb.RamBlockMetadataStore;
import org.peergos.blockstore.metadatadb.sql.H2Commands;
import org.peergos.blockstore.metadatadb.sql.SqliteCommands;
import org.peergos.blockstore.metadatadb.sql.UncloseableConnection;
import org.peergos.blockstore.s3.S3Blockstore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

/*
To run set up minIO and mc. ie for mac
brew install minio/stable/minio
brew install minio/stable/mc

export MINIO_ROOT_USER=test
export MINIO_ROOT_PASSWORD=testdslocal
minio server /Users/Shared/minio
mc alias set minio 'http://local-s3.localhost:9000' 'test' 'testdslocal'
 */
public class S3BlockStoreTest {

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
        BlockMetadataStore metadata =  new JdbcBlockMetadataStore(() -> instance, new H2Commands());
        testFileStore(metadata);
    }

    public void testFileStore(BlockMetadataStore metadata) {
        Map<String, Object> params = new HashMap<>();
        params.put("type", "s3ds");
        params.put("region", "local");
        params.put("bucket", "local-s3");
        params.put("rootDirectory", "");
        params.put("regionEndpoint", "localhost:9000");
        params.put("accessKey", "test");
        params.put("secretKey", "testdslocal");

        S3Blockstore bs = new S3Blockstore(params, metadata);

        String msg = "hello world!";
        byte[] block = msg.getBytes();
        Cid.Codec codec = Cid.Codec.Raw;
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));

        //clean up from any previous failed test
        bs.rm(cid).join();

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

        //List<Cid> allCids =  bs.refs().join();
        //Assert.assertTrue("ref found", !allCids.isEmpty());
        //Assert.assertTrue("cid retrieved", !allCids.stream().filter(c -> c.equals(msg)).collect(Collectors.toList()).isEmpty());
        bs.rm(cid).join();
        found = bs.has(cid).join();
        Assert.assertTrue("Found cid", !found);
    }

}
