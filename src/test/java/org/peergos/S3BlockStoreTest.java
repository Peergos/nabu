package org.peergos;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.junit.*;
import org.peergos.blockstore.FileBlockstore;
import org.peergos.blockstore.S3Blockstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

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
    @Ignore
    public void testFileStore() {
        Map<String, Object> params = new HashMap<>();
        params.put("type", "s3ds");
        params.put("region", "local");
        params.put("bucket", "local-s3");
        params.put("rootDirectory", "");
        params.put("regionEndpoint", "localhost:9000");
        params.put("accessKey", "test");
        params.put("secretKey", "testdslocal");

        S3Blockstore bs = new S3Blockstore(params);
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

        //List<Cid> allCids =  bs.refs().join();
        //Assert.assertTrue("ref found", !allCids.isEmpty());
        //Assert.assertTrue("cid retrieved", !allCids.stream().filter(c -> c.equals(msg)).collect(Collectors.toList()).isEmpty());
        bs.rm(cid).join();
        found = bs.has(cid).join();
        Assert.assertTrue("Found cid", !found);
    }

}
