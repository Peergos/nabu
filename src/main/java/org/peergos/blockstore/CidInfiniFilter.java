package org.peergos.blockstore;

import io.ipfs.cid.*;
import org.peergos.blockstore.filters.*;

import java.util.*;

public class CidInfiniFilter implements Filter {

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
        return build(bs, 0.01);
    }

    public static CidInfiniFilter build(Blockstore bs, double falsePositiveRate) {
        List<Cid> refs = bs.refs().join();
        int nBlocks = refs.size();
        int nextPowerOfTwo = Math.max(17, (int) (1 + Math.log(nBlocks) / Math.log(2)));
        double expansionAlpha = 0.8;
        int bitsPerEntry = (int)(4 - Math.log(falsePositiveRate / expansionAlpha) / Math.log(2) + 1);
        ChainedInfiniFilter infini = new ChainedInfiniFilter(nextPowerOfTwo, bitsPerEntry);
        infini.set_expand_autonomously(true);
        refs.forEach(c -> infini.insert(c.toBytes(), true));
        return new CidInfiniFilter(infini);
    }
}
