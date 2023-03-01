package org.peergos;
import io.ipfs.cid.Cid;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.peergos.protocol.dht.DatabaseRecordStore;
import org.peergos.protocol.ipns.IpnsRecord;

import java.time.LocalDateTime;
import java.util.Optional;
public class DatabaseRecordStoreTest {

    @Test
    public void testRecordStore() {
        try (DatabaseRecordStore bs = new DatabaseRecordStore("mem:")) {
            LocalDateTime now = LocalDateTime.now();
            IpnsRecord record = new IpnsRecord("raw".getBytes(), 1, 2, now, "value");
            Cid peerId = Cid.decode("zb2rhYSxw4ZjuzgCnWSt19Q94ERaeFhu9uSqRgjSdx9bsgM6f");
            bs.put(peerId, record);
            //make sure PUTing a second time succeeds
            bs.put(peerId, record);

            Optional<IpnsRecord> result = bs.get(peerId);
            Assert.assertTrue("IpnsRecord found", !result.isEmpty());
            IpnsRecord retrievedRecord = result.get();
            Assert.assertTrue("IpnsRecord raw match", new String(retrievedRecord.raw).equals(new String(record.raw)));
            Assert.assertTrue("IpnsRecord seq match", Long.valueOf(retrievedRecord.sequence).equals(Long.valueOf(record.sequence)));
            Assert.assertTrue("IpnsRecord ttl match", Long.valueOf(retrievedRecord.ttlNanos).equals(Long.valueOf(record.ttlNanos)));
            LocalDateTime expiry = retrievedRecord.expiry.plusNanos(record.expiry.getNano());
            Assert.assertTrue("IpnsRecord ldt match", expiry.equals(record.expiry));
            Assert.assertTrue("IpnsRecord value match", new String(retrievedRecord.value).equals(new String(record.value)));

            bs.remove(peerId);
            Optional<IpnsRecord> deleted = bs.get(peerId);
            Assert.assertTrue("IpnsRecord not deleted", deleted.isEmpty());

        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
