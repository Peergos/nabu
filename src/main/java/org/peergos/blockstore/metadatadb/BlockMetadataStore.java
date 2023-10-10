package org.peergos.blockstore.metadatadb;

import io.ipfs.cid.Cid;
import org.peergos.cbor.CborObject;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface BlockMetadataStore {

    Optional<BlockMetadata> get(Cid block);

    void put(Cid block, BlockMetadata meta);

    void remove(Cid block);

    long size();

    Stream<Cid> list();

    Stream<Cid> listCbor();

    default BlockMetadata put(Cid block, byte[] data) {
        BlockMetadata meta = extractMetadata(block, data);
        put(block, meta);
        return meta;
    }

    static BlockMetadata extractMetadata(Cid block, byte[] data) {
        if (block.codec == Cid.Codec.Raw) {
            BlockMetadata meta = new BlockMetadata(data.length, Collections.emptyList());
            return meta;
        } else if(block.codec == Cid.Codec.DagCbor){
            CborObject cbor = CborObject.fromByteArray(data);
            List<Cid> links = cbor
                    .links().stream()
                    .map(h -> (Cid) h)
                    .collect(Collectors.toList());
            BlockMetadata meta = new BlockMetadata(data.length, links);
            return meta;
        } else {
            throw new IllegalStateException("Unsupported Block type");
        }
    }

    void compact();
}
