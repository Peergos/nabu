package org.peergos.blockstore;

import io.ipfs.cid.*;
import org.peergos.blockstore.filters.*;
import org.peergos.util.Logging;

import java.util.*;
import java.util.logging.*;

public class CidInfiniFilter implements Filter {

    private static final Logger LOG = Logging.LOG();

    private final ChainedInfiniFilter filter;

    private CidInfiniFilter(ChainedInfiniFilter filter) {
        this.filter = filter;
    }

    @Override
    public boolean has(Cid c) {
        return filter.search(c.toBytes());
    }

    @Override
    public Cid add(Cid c) {
        filter.insert(c.toBytes(), true);
        return c;
    }

    public static CidInfiniFilter build(Blockstore bs) {
        return build(bs, 0.0001);
    }

    public static CidInfiniFilter build(Blockstore bs, double falsePositiveRate) {
        long nBlocks = bs.count(false).join()*5/4; //  increase by 25% to avoid expansion during build
        LOG.info("Building infini filter for " + nBlocks + " blocks with false positive rate: " + falsePositiveRate);
        if (nBlocks > Integer.MAX_VALUE)
            throw new IllegalStateException("Too many blocks! " + nBlocks);
        CidInfiniFilter infini = build((int)nBlocks, falsePositiveRate);
        bs.applyToAll(infini::add, false);
        return infini;
    }

    public static CidInfiniFilter build(int nBlocks, double falsePositiveRate) {
        int nextPowerOfTwo = Math.max(17, (int) (1 + Math.log(nBlocks) / Math.log(2)));
        double expansionAlpha = 0.8;
        int bitsPerEntry = (int)(4 - Math.log(falsePositiveRate / expansionAlpha) / Math.log(2) + 1);
        LOG.info("Using infini filter of initial size " + ((double)(bitsPerEntry * (1 << nextPowerOfTwo) / 8) / 1024 / 1024) + " MiB");
        ChainedInfiniFilter infini = new ChainedInfiniFilter(nextPowerOfTwo, bitsPerEntry);
        infini.set_expand_autonomously(true);
        return new CidInfiniFilter(infini);
    }
}
