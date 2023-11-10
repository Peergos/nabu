package org.peergos.blockstore;

import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import org.junit.*;

import java.util.*;

public class BloomTest {
    private static Random r = new Random(42);

    @Test
    public void bloom() {
        RamBlockstore bs = new RamBlockstore();
        int nBlocks = 100_000;
        addRandomBlocks(nBlocks, bs);

        long t1 = System.currentTimeMillis();
        CidBloomFilter bloom = CidBloomFilter.build(bs);
        long t2 = System.currentTimeMillis();
        System.out.println("Building filter took: " + (t2-t1)+ "ms");
        List<Cid> refs = bs.refs(false).join();
        for (Cid ref : refs) {
            Assert.assertTrue(bloom.has(ref));
        }

        checkFalsePositiveRate(bloom, 1.1);

        // double the blockstore size and check the false positive rate
        FilteredBlockstore filtered = new FilteredBlockstore(bs, bloom);
        long t3 = System.currentTimeMillis();
        addRandomBlocks(nBlocks, filtered);
        long t4 = System.currentTimeMillis();
        System.out.println("Doubling filter size took: " + (t4-t3)+ "ms");

        checkFalsePositiveRate(bloom, 14);
    }

    private static void addRandomBlocks(int nBlocks, Blockstore b) {
        for (int i = 0; i < nBlocks; i++) {
            byte[] block = new byte[10];
            r.nextBytes(block);
            b.put(block, Cid.Codec.Raw);
        }
    }

    private static void checkFalsePositiveRate(CidBloomFilter bloom, double tolerance) {
        int in = 0;
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            byte[] hash = new byte[32];
            r.nextBytes(hash);
            Cid random = new Cid(1, Cid.Codec.Raw, Multihash.Type.sha2_256, hash);
            if (bloom.has(random))
                in++;
        }
        double falsePositiveRate = 0.01;
        Assert.assertTrue(in < total * falsePositiveRate * tolerance);
    }
}
