package org.peergos.blockstore.filters.bitmap;

public abstract class Bitmap {
	
	public abstract long size();
	public abstract void set(long bit_index, boolean value);
	public abstract void setFromTo(long from, long to, long value);
	public abstract boolean get(long bit_index);
	public abstract long getFromTo(long from, long to);
	
	public static boolean get_fingerprint_bit(long index, long fingerprint) {
		long mask = 1 << index;
		long and = fingerprint & mask;
		return and != 0;
	}
	
}
