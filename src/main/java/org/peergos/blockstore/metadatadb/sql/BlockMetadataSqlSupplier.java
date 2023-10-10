package org.peergos.blockstore.metadatadb.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public interface BlockMetadataSqlSupplier {

    String getByteArrayType();


    String sqlInteger();

    String addMetadataCommand();

    String vacuumCommand();

    default String createBlockMetadataStoreTableCommand() {
        return "CREATE TABLE IF NOT EXISTS blockmetadata (cid " + getByteArrayType() + " primary key not null, " +
                "size " + sqlInteger() + " not null, " +
                "links " + getByteArrayType() + " not null);";
    }

    default void createTable(String sqlTableCreate, Connection conn) throws SQLException {
        Statement createStmt = conn.createStatement();
        createStmt.executeUpdate(sqlTableCreate);
        createStmt.close();
    }
}
