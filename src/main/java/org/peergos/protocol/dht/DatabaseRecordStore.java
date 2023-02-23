package org.peergos.protocol.dht;
import io.ipfs.cid.Cid;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import org.peergos.protocol.ipns.IpnsRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

public class DatabaseRecordStore implements RecordStore, AutoCloseable {

    private final String connectionStringPrefix = "jdbc:h2:";//./store/records;AUTO_RECONNECT=TRUE
    private final Connection connection;

    private final String RECORD_TABLE = "records";
    private final int SIZE_OF_VAL = 1000;
    private final int SIZE_OF_PEERID = 100;

    /*
     * Constructs a DatabaseRecordStore object
     * @param location - location of the database on disk (See: https://h2database.com/html/cheatSheet.html for options)
     */
    public DatabaseRecordStore(String location) {
        try {
            this.connection = getConnection(connectionStringPrefix + location);
            this.connection.setAutoCommit(true);
            createTable();
        } catch (SQLException sqle) {
            throw new IllegalStateException(sqle);
        }
    }
    public void close() throws Exception {
        connection.close();
    }

    private Connection getConnection(String connectionString) throws SQLException {
        return DriverManager.getConnection(connectionString);
    }

    private void createTable() {
        String createSQL = "create table if not exists " + RECORD_TABLE
                + " (peerId VARCHAR(" + SIZE_OF_PEERID + ") primary key not null, raw BLOB not null, "
                + "sequence BIGINT not null, ttlNanos BIGINT not null, expiryUTC BIGINT not null, "
                + "val VARCHAR(" + SIZE_OF_VAL + ") not null);";
        try (PreparedStatement select = connection.prepareStatement(createSQL)) {
            select.execute();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private String hashToKey(Multihash hash) {
        String padded = new Base32().encodeAsString(hash.toBytes());
        int padStart = padded.indexOf("=");
        return padStart > 0 ? padded.substring(0, padStart) : padded;
    }

    @Override
    public Optional<IpnsRecord> get(Cid peerId) {
        String selectSQL = "SELECT raw, sequence, ttlNanos, expiryUTC, val FROM " + RECORD_TABLE + " WHERE peerId=?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, hashToKey(peerId));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    try (InputStream input = rs.getBinaryStream("raw")) {
                        byte[] buffer = new byte[1024];
                        ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        for (int len; (len = input.read(buffer)) != -1; ) {
                            bout.write(buffer, 0, len);
                        }
                        LocalDateTime expiry = LocalDateTime.ofEpochSecond(rs.getLong("expiryUTC"),
                                0, ZoneOffset.UTC);
                        IpnsRecord record = new IpnsRecord(bout.toByteArray(), rs.getLong("sequence"),
                                rs.getLong("ttlNanos"),  expiry, rs.getString("val"));
                        return Optional.of(record);
                    } catch (IOException readEx) {
                        throw new IllegalStateException(readEx);
                    }
                } else {
                    return Optional.empty();
                }
            } catch (SQLException rsEx) {
                throw new IllegalStateException(rsEx);
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException(sqlEx);
        }
    }

    @Override
    public void put(Multihash peerId, IpnsRecord record) {
        String updateSQL = "MERGE INTO " + RECORD_TABLE
                + " (peerId, raw, sequence, ttlNanos, expiryUTC, val) VALUES (?, ?, ?, ?, ?, ?);";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, hashToKey(peerId));
            pstmt.setBytes(2, record.raw);
            pstmt.setLong(3, record.sequence);
            pstmt.setLong(4, record.ttlNanos);
            pstmt.setLong(5, record.expiry.toEpochSecond(ZoneOffset.UTC));
            pstmt.setString(6, record.value.length() > SIZE_OF_VAL ?
                    record.value.substring(0, SIZE_OF_VAL) : record.value);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void remove(Multihash peerId) {
        String deleteSQL = "DELETE FROM " + RECORD_TABLE + " WHERE peerId=?";
        try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
            pstmt.setString(1, hashToKey(peerId));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }
}

