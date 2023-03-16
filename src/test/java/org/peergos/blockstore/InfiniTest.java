package org.peergos.blockstore;

import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import org.junit.*;

import java.util.*;

public class InfiniTest {
    private static Random r = new Random(42);

    @Test
    public void infini() {
        RamBlockstore bs = new RamBlockstore();
        int nBlocks = 65_000;
        addRandomBlocks(nBlocks, bs);

        long t1 = System.currentTimeMillis();
        CidInfiniFilter infini = CidInfiniFilter.build(bs);
        long t2 = System.currentTimeMillis();
        System.out.println("Building filter took: " + (t2-t1)+ "ms");
        List<Cid> refs = bs.refs().join();
        for (Cid ref : refs) {
            Assert.assertTrue(infini.has(ref));
        }

        checkFalsePositiveRate(infini, 1.1);

        // double the blockstore size and check the false positive rate
        FilteredBlockstore filtered = new FilteredBlockstore(bs, infini);
        long t3 = System.currentTimeMillis();
        addRandomBlocks(nBlocks, filtered);
        long t4 = System.currentTimeMillis();
        System.out.println("Doubling filter size took: " + (t4-t3)+ "ms");

        checkFalsePositiveRate(infini, 1.1);
    }

    private static void addRandomBlocks(int nBlocks, Blockstore b) {
        for (int i = 0; i < nBlocks; i++) {
            byte[] block = new byte[10];
            r.nextBytes(block);
            b.put(block, Cid.Codec.Raw);
        }
    }

    private static void checkFalsePositiveRate(CidInfiniFilter bloom, double tolerance) {
        int in = 0;
        int total = 10_000;
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
