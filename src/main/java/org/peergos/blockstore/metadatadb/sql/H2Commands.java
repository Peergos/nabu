package org.peergos.blockstore.metadatadb.sql;

public class H2Commands implements SqlSupplier {

    @Override
    public String vacuumCommand() {
        return "";
    }

    @Override
    public String addMetadataCommand() {
        return "INSERT INTO blockmetadata (cid, size, links) VALUES(?, ?, ?) ON CONFLICT DO NOTHING;";
    }

    @Override
    public String getByteArrayType() {
        return "OBJECT";
    }

    @Override
    public String sqlInteger() {
        return "BIGINT";
    }
}
