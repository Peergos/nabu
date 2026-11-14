package org.peergos;

import org.junit.*;
import org.peergos.cbor.*;
import org.peergos.util.*;

import java.lang.management.*;
import java.util.*;

public class CborObjectTest {

    private static byte[] nested(String levelHex, int levels, String leafHex) {
        byte[] level = ArrayOps.hexToBytes(levelHex), leaf = ArrayOps.hexToBytes(leafHex);
        byte[] res = new byte[level.length * levels + leaf.length];
        for (int i=0; i < levels; i++)
            System.arraycopy(level, 0, res, i * level.length, level.length);
        System.arraycopy(leaf, 0, res, level.length * levels, leaf.length);
        return res;
    }

    private static void assertRejected(byte[] cbor) {
        try {
            CborObject.fromByteArray(cbor);
            Assert.fail("Should have failed!");
        } catch (IllegalStateException expected) {}
    }

    @Test
    public void deepNesting() {
        // a 64 KiB input of arrays each declaring 65,535 items
        assertRejected(Arrays.copyOf(nested("99ffff", 4_000, ""), 64 * 1024));
        // merkle link tags
        assertRejected(nested("d82a", 30_000, ""));
        // {"": {"": ... 0}}
        byte[] deepest = nested("a160", CborObject.MAX_DEPTH, "00");
        Assert.assertArrayEquals(deepest, CborObject.fromByteArray(deepest).toByteArray());
        assertRejected(nested("a160", CborObject.MAX_DEPTH + 1, "00"));
    }

    @Test
    public void declaredArrayLengthsAreNotPreallocated() {
        // a 1 MiB input of arrays nested to the limit, each declaring a million items, then an invalid item
        byte[] cbor = new byte[1024 * 1024];
        Arrays.fill(cbor, (byte) 0xff);
        byte[] arrays = nested("9a000fffff", CborObject.MAX_DEPTH, "");
        System.arraycopy(arrays, 0, cbor, 0, arrays.length);
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long before = threads.getCurrentThreadAllocatedBytes();
        assertRejected(cbor);
        long allocated = threads.getCurrentThreadAllocatedBytes() - before;
        Assert.assertTrue("Allocated " + allocated + " bytes", allocated < cbor.length);
    }
}
