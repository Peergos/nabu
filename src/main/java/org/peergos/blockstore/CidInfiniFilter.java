package org.peergos.blockstore;

import io.ipfs.cid.*;
import org.peergos.blockstore.filters.*;

import java.util.*;

public class CidInfiniFilter implements Filter {

    private final ChainedInfiniFilter filter;

    public CidInfiniFilter(ChainedInfiniFilter filter) {
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
        List<Cid> refs = bs.refs().join();
        int nBlocks = refs.size();
        int nextPowerOfTwo = Math.max(17, (int) (1 + Math.log(nBlocks) / Math.log(2)));
        double falsePositiveRate = 0.01;
        int bitsPerEntry = (int)(1 - Math.log(falsePositiveRate /(1 + Math.log(nBlocks)/Math.log(2)))/Math.log(2));
        ChainedInfiniFilter infini = new ChainedInfiniFilter(nextPowerOfTwo, bitsPerEntry);
        refs.forEach(c -> infini.insert(c.toBytes(), true));
        return new CidInfiniFilter(infini);
    }
}
