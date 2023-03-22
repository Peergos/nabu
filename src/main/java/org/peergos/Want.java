package org.peergos;

import io.ipfs.cid.Cid;

import java.util.*;

public class Want {
    public final Cid cid;
    public final Optional<String> auth;
    public Want(Cid cid, Optional<String> auth) {
        this.cid = cid;
        this.auth = auth;
    }

    public Want(Cid h) {
        this(h, Optional.empty());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Want want = (Want) o;
        return cid.equals(want.cid) && auth.equals(want.auth);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cid, auth);
    }
}
