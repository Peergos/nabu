package org.peergos;

import io.ipfs.cid.Cid;

public class Want {
    public final Cid cid;
    public final String auth;
    public Want(Cid cid, String auth) {
        this.cid = cid;
        this.auth = auth;
    }
}
