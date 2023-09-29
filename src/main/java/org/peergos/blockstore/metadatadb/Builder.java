package org.peergos.blockstore.metadatadb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.peergos.Args;
import org.peergos.blockstore.metadatadb.sql.PostgresCommands;
import org.peergos.blockstore.metadatadb.sql.SqliteCommands;
import org.peergos.util.Sqlite;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

public class Builder {

    public static Supplier<Connection> getPostgresConnector(Args a, String prefix) {
        String postgresHost = a.getArg(prefix + "postgres.host");
        int postgresPort = a.getInt(prefix + "postgres.port", 5432);
        String databaseName = a.getArg(prefix + "postgres.database", "peergos");
        String postgresUsername = a.getArg(prefix + "postgres.username");
        String postgresPassword = a.getArg(prefix + "postgres.password");

        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource");
        props.setProperty("dataSource.serverName", postgresHost);
        props.setProperty("dataSource.portNumber", "" + postgresPort);
        props.setProperty("dataSource.user", postgresUsername);
        props.setProperty("dataSource.password", postgresPassword);
        props.setProperty("dataSource.databaseName", databaseName);
        HikariConfig config = new HikariConfig(props);
        HikariDataSource ds = new HikariDataSource(config);

        return () -> {
            try {
                return ds.getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static BlockMetadataStore buildBlockMetadata(Args a) {
        try {
            boolean usePostgres = a.getArg("block-metadata-db-type", "sqlite").equals("postgres");
            if (usePostgres) {
                return new JdbcBlockMetadataStore(getPostgresConnector(a, "metadb."), new PostgresCommands());
            } else {
                File metaFile = a.fromIPFSDir("block-metadata-sql-file", "blockmetadata.sql").toFile();
                Connection instance = new Sqlite.UncloseableConnection(Sqlite.build(metaFile.getPath()));
                return new JdbcBlockMetadataStore(() -> instance, new SqliteCommands());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
