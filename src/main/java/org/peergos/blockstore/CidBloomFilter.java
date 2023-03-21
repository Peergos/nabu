package org.peergos.blockstore;

import io.ipfs.cid.*;

import java.util.*;

public class CidBloomFilter implements Filter {

    private final BloomFilter<Cid> bloom;

    public CidBloomFilter(BloomFilter<Cid> bloom) {
        this.bloom = bloom;
    }

    @Override
    public boolean has(Cid c) {
        return bloom.contains(c);
    }

    @Override
    public Cid add(Cid c) {
        bloom.add(c);
        return c;
    }

    public static CidBloomFilter build(Blockstore bs, double falsePositiveRate) {
        List<Cid> refs = bs.refs().join();
        BloomFilter<Cid> bloom = new BloomFilter<>(falsePositiveRate, refs.size());
        refs.forEach(bloom::add);
        return new CidBloomFilter(bloom);
    }

    public static CidBloomFilter build(Blockstore bs) {
        return build(bs, 0.01);
    }
}
