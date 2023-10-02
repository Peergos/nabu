package org.peergos.blockstore.metadatadb.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public interface SqlSupplier {

    String listTablesCommand();

    String tableExistsCommand();

    String getByteArrayType();

    String getSerialIdType();

    String sqlInteger();

    String ensureColumnExistsCommand(String table, String column, String type);

    String addMetadataCommand();

    String vacuumCommand();

    default String createBlockMetadataStoreTableCommand() {
        return "CREATE TABLE IF NOT EXISTS blockmetadata (cid " + getByteArrayType() + " primary key not null, " +
                "size " + sqlInteger() + " not null, " +
                "links " + getByteArrayType() + " not null);";
    }

    String insertOrIgnoreCommand(String prefix, String suffix);

    default void createTable(String sqlTableCreate, Connection conn) throws SQLException {
        Statement createStmt = conn.createStatement();
        createStmt.executeUpdate(sqlTableCreate);
        createStmt.close();
    }
}
