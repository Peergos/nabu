package org.peergos;

import io.ipfs.cid.Cid;

import java.util.*;

public class Want {
    public final Cid cid;
    public final Optional<String> authHex;
    public Want(Cid cid, Optional<String> authHex) {
        this.cid = cid;
        this.authHex = authHex.flatMap(a -> a.isEmpty() ? Optional.empty() : Optional.of(a));
    }

    public Want(Cid h) {
        this(h, Optional.empty());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Want want = (Want) o;
        return cid.equals(want.cid) && authHex.equals(want.authHex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cid, authHex);
    }
}
