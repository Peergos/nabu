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

    public static CidBloomFilter build(Blockstore bs) {
        List<Cid> refs = bs.refs().join();
        BloomFilter<Cid> bloom = new BloomFilter<>(0.01, refs.size());
        refs.forEach(bloom::add);
        return new CidBloomFilter(bloom);
    }
}
