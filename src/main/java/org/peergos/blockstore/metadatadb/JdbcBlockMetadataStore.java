package org.peergos.blockstore.metadatadb;


import io.ipfs.cid.Cid;
import org.peergos.blockstore.metadatadb.sql.SqlSupplier;
import org.peergos.cbor.CborObject;
import org.peergos.util.Logging;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcBlockMetadataStore implements BlockMetadataStore {

    private static final Logger LOG = Logging.LOG();
    private static final String GET_INFO = "SELECT * FROM blockmetadata WHERE cid = ?;";
    private static final String REMOVE = "DELETE FROM blockmetadata where cid = ?;";
    private static final String LIST = "SELECT cid FROM blockmetadata;";
    private static final String SIZE = "SELECT COUNT(*) FROM blockmetadata;";
    private Supplier<Connection> conn;
    private final SqlSupplier commands;

    public JdbcBlockMetadataStore(Supplier<Connection> conn, SqlSupplier commands) {
        this.conn = conn;
        this.commands = commands;
        init(commands);
    }

    private Connection getConnection() {
        return getConnection(true, true);
    }

    private Connection getConnection(boolean autocommit, boolean serializable) {
        Connection connection = conn.get();
        try {
            if (autocommit)
                connection.setAutoCommit(true);
            if (serializable)
                connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void init(SqlSupplier commands) {
        try (Connection conn = getConnection()) {
            commands.createTable(commands.createBlockMetadataStoreTableCommand(), conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void compact() {
        String vacuum = commands.vacuumCommand();
        if (vacuum.isEmpty())
            return;
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(vacuum)) {
            stmt.executeUpdate();
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    public void remove(Cid block) {
        try (Connection conn = getConnection();
             PreparedStatement remove = conn.prepareStatement(REMOVE)) {

            remove.setBytes(1, block.toBytes());
            remove.executeUpdate();
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public Optional<BlockMetadata> get(Cid block) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(GET_INFO)) {
            stmt.setBytes(1, block.toBytes());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                List<Cid> links = ((CborObject.CborList) CborObject.fromByteArray(rs.getBytes("links")))
                        .map(cbor -> Cid.cast(((CborObject.CborByteArray)cbor).value));
                return Optional.of(new BlockMetadata(rs.getInt("size"), links));
            }
            return Optional.empty();
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public void put(Cid block, BlockMetadata meta) {
        try (Connection conn = getConnection();
             PreparedStatement insert = conn.prepareStatement(commands.addMetadataCommand())) {

            insert.setBytes(1, block.toBytes());
            insert.setLong(2, meta.size);
            insert.setBytes(3, new CborObject.CborList(meta.links.stream()
                    .map(Cid::toBytes)
                    .map(CborObject.CborByteArray::new)
                    .collect(Collectors.toList()))
                    .toByteArray());
            insert.executeUpdate();
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public long size() {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(SIZE)) {
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public Stream<Cid> list() {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(LIST)) {
            ResultSet rs = stmt.executeQuery();
            List<Cid> res = new ArrayList<>();
            while (rs.next()) {
                res.add(Cid.cast(rs.getBytes("cid")));
            }
            return res.stream();
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }

    @Override
    public Stream<Cid> listCbor() {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(LIST)) {
            ResultSet rs = stmt.executeQuery();
            List<Cid> res = new ArrayList<>();
            while (rs.next()) {
                Cid cid = Cid.cast(rs.getBytes("cid"));
                if (cid.codec != Cid.Codec.Raw) {
                    res.add(cid);
                }
            }
            return res.stream();
        } catch (SQLException sqe) {
            LOG.log(Level.WARNING, sqe.getMessage(), sqe);
            throw new RuntimeException(sqe);
        }
    }
}
