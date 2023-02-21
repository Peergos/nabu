package org.peergos.blockstore;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.peergos.Hash;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class SQLiteBlockstore implements Blockstore {
    private static final Logger LOG = Logger.getLogger(FileBlockstore.class.getName());

    final String pathToSQLiteFile;
    final String BLOCK_STORE_TABLE = "blocks";

    public SQLiteBlockstore(String pathToSQLiteFile) {
        if (pathToSQLiteFile == null) {
            throw new IllegalArgumentException("filePath cannot be NULL");
        }
        this.pathToSQLiteFile = pathToSQLiteFile;
        createTable();
        LOG.info("Using SQLiteBlockstore at location: " + pathToSQLiteFile);
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + pathToSQLiteFile);
    }

    private void createTable() {
        String createSQL = "create table if not exists " + BLOCK_STORE_TABLE + " (hash text primary key not null, data blob not null);";
        try (Connection connection = getConnection();
             PreparedStatement select = connection.prepareStatement(createSQL)) {
            select.execute();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }


    @Override
    public CompletableFuture<Boolean> has(Cid c) {
        String selectSQL = "SELECT 1 FROM " + BLOCK_STORE_TABLE + " WHERE hash=?";
        try (Connection connection = getConnection();
             PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, hashToKey(c));
            try (ResultSet rs = pstmt.executeQuery()) {
                return CompletableFuture.completedFuture(rs.next());
            } catch (SQLException rsEx) {
                throw new IllegalStateException(rsEx);
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException(sqlEx);
        }
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(Cid c) {
        String selectSQL = "SELECT data FROM " + BLOCK_STORE_TABLE + " WHERE hash=?";
        try (Connection connection = getConnection();
             PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, hashToKey(c));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    try (InputStream input = rs.getBinaryStream("data")) {
                        byte[] buffer = new byte[1024];
                        ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        for (int len; (len = input.read(buffer)) != -1; ) {
                            bout.write(buffer, 0, len);
                        }
                        return CompletableFuture.completedFuture(Optional.of(bout.toByteArray()));
                    } catch (IOException readEx) {
                        throw new IllegalStateException(readEx);
                    }
                } else {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
            } catch (SQLException rsEx) {
                throw new IllegalStateException(rsEx);
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException(sqlEx);
        }
    }

    @Override
    public CompletableFuture<Cid> put(byte[] block, Cid.Codec codec) {
        Cid cid = new Cid(1, codec, Multihash.Type.sha2_256, Hash.sha256(block));
        String updateSQL = "INSERT OR IGNORE INTO " + BLOCK_STORE_TABLE
                + " (hash, data) VALUES (?, ?);";
        try (Connection connection = getConnection();
             PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, hashToKey(cid));
            pstmt.setBytes(2, block);
            pstmt.executeUpdate();
            return CompletableFuture.completedFuture(cid);
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
