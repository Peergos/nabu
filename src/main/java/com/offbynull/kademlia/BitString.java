/*
 * Copyright (c) 2017, Kasra Faghihi, All rights reserved.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */
package com.offbynull.kademlia;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.commons.lang3.Validate;

/**
 * Bit string.
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class BitString implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final byte[] data; // treated as an array of bit starting from bit 0
    private final int bitLength;
    
    // make sure that whatever you pass in as data is a copy / not-shared.
    private BitString(byte[] data, int bitLength) {
        Validate.notNull(data);
        Validate.isTrue(bitLength >= 0);
        
        int minLength = calculateRequiredByteArraySize(bitLength);
        Validate.isTrue(data.length == minLength);
        
        this.data = data;
        this.bitLength = bitLength;
    }

    /**
     * Constructs a {@link BitString} from a string. Bits from string are read in read-order (starting from position 0 onward). Character
     * {@code '0'} is represented as bit {@code 0}, and character {@code '1'} is represented as bit {@code 1}.
     * @param data string to read bitstring data from
     * @return created bitstring
     * @throws IllegalArgumentException if any chracter other than {@code '0'} or {@code '1'} is encountered in {@code data}
     * @throws NullPointerException if any argument is {@code null}
     */
    public static BitString createFromString(String data) {
        Validate.notNull(data);
        
        int offset = 0;
        int len = data.length();
        
        int arrLen = calculateRequiredByteArraySize(len);
        int arrIdx = 0;
        byte[] arr = new byte[arrLen];
        
        int end = offset + len;
        int currOffset = offset;
        byte[] temp = new byte[1];
        while (currOffset < end) {
            int nextOffset = Math.min(currOffset + 8, end);
            int currLen = nextOffset - currOffset;
            
            try {
                temp[0] = (byte) (Integer.valueOf(data.substring(currOffset, currOffset + currLen), 2) & 0xFF);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException(nfe);
            }
            
            if (currLen < 8) {
                temp[0] = (byte) ((temp[0] << (8 - currLen)) & 0xFF);
            }
            
            byte b = readBitsFromByteArrayInReadOrder(temp, 0, currLen);
            arr[arrIdx] = b;

            arrIdx++;
            currOffset = nextOffset;
        }
        
        return new BitString(arr, len);
    }

    /**
     * Constructs a {@link BitString} from a byte array. Bits from input array are read in logical-order (starting from bit 0 onward). This
     * seems counter-intuitive because an array of bytes is represented from left-to-right (byte 0 is leftmost) while an array of bits is
     * represented from right-to-left (bit 0 is the right-most). So for example, the input array {@code [0x04, 0xFB]} with offset of 2 and
     * length of 10 would result in bitstring {@code 1000 0011 01}.
     * <p>
     * Read-order representation {@code [0x01, 0xFB]}, where bits are ordered from right-to-left...
     * <pre>
     * Byte    0                 1
     * Bit     7 6 5 4 3 2 1 0   7 6 5 4 3 2 1 0
     *         ---------------------------------
     *         0 0 0 0 0 1 0 0   1 1 1 1 1 0 1 1
     *         0       4         F       B
     * </pre>
     * Logical-order representation {@code [0x01, 0xFB]}, where bits are ordered from left-to-right ...
     * <pre>
     * Byte    0                 1
     * Bit     0 1 2 3 4 5 6 7   0 1 2 3 4 5 6 7
     *         ---------------------------------
     *         0 0 1 0 0 0 0 0   1 1 0 1 1 1 1 1
     *               4       0         B       F
     *             ^                   ^
     *             |                   |
     *          offset (2)        offset+len (2+10)
     * </pre>
     * The logical-order representation is the way bits are read in.
     * @param data array to read bitstring data from
     * @param offset bit position to read from
     * @param len number of bits to read
     * @return created bitstring
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if {@code bitLength <= 0}, or if {@code data} is larger than the minimum number of bytes that it
     * takes to retain {@code bitLength} (e.g. if you're retaining 12 bits, you need 2 bytes or less -- {@code 12/8 + (12%8 == 0 ? 0 : 1)})
     */
    public static BitString createLogicalOrder(byte[] data, int offset, int len) {
        Validate.notNull(data);
        Validate.isTrue(offset >= 0);
        Validate.isTrue(len >= 0);
        Validate.isTrue(offset + len <= data.length * 8);
        
        int arrLen = calculateRequiredByteArraySize(len);
        int arrIdx = 0;
        byte[] arr = new byte[arrLen];
        
        int end = offset + len;
        int currOffset = offset;
        while (currOffset < end) {
            int nextOffset = Math.min(currOffset + 8, end);
            int currLen = nextOffset - currOffset;
            
            byte b = readBitsFromByteArrayInLogicalOrder(data, currOffset, currLen);
            arr[arrIdx] = b;

            arrIdx++;
            currOffset = nextOffset;
        }
        
        return new BitString(arr, len);
    }

    /**
     * Constructs a {@link BitString} from a byte array. Bits from input array are read in read-order (the order you would read a bytes in
     * a byte array). So for example, the input array {@code [0x04, 0xFB]} with offset of 2 and length of 10 would result in bitstring
     * {@code 0001 0011 11}.
     * <p>
     * Read-order representation {@code [0x01, 0xFB]}, where bits are ordered from right-to-left...
     * <pre>
     * Byte    0                 1
     * Bit     7 6 5 4 3 2 1 0   7 6 5 4 3 2 1 0
     *         ---------------------------------
     *         0 0 0 0 0 1 0 0   1 1 1 1 1 0 1 1
     *         0       4         F       B
     *             ^                   ^
     *             |                   |
     *          offset (2)        offset+len (2+10)
     * </pre>
     * Logical-order representation {@code [0x01, 0xFB]}, where bits are ordered from left-to-right ...
     * <pre>
     * Byte    0                 1
     * Bit     0 1 2 3 4 5 6 7   0 1 2 3 4 5 6 7
     *         ---------------------------------
     *         0 0 1 0 0 0 0 0   1 1 0 1 1 1 1 1
     *               4       0         B       F
     * </pre>
     * The read-order representation is the way bits are read in.
     * @param data array to read bitstring data from
     * @param offset bit position to read from
     * @param len number of bits to read
     * @return created bitstring
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if {@code bitLength <= 0}, or if {@code data} is larger than the minimum number of bytes that it
     * takes to retain {@code bitLength} (e.g. if you're retaining 12 bits, you need 2 bytes or less -- {@code 12/8 + (12%8 == 0 ? 0 : 1)})
     */
    public static BitString createReadOrder(byte[] data, int offset, int len) {
        Validate.notNull(data);
        Validate.isTrue(offset >= 0);
        Validate.isTrue(len >= 0);
        Validate.isTrue(offset + len <= data.length * 8);
        
        int arrLen = calculateRequiredByteArraySize(len);
        int arrIdx = 0;
        byte[] arr = new byte[arrLen];
        
        int end = offset + len;
        int currOffset = offset;
        while (currOffset < end) {
            int nextOffset = Math.min(currOffset + 8, end);
            int currLen = nextOffset - currOffset;
            
            byte b = readBitsFromByteArrayInReadOrder(data, currOffset, currLen);
            arr[arrIdx] = b;

            arrIdx++;
            currOffset = nextOffset;
        }
        
        return new BitString(arr, len);
    }
    
    // Why work on longs? It may be more efficient to have the byte[] containing the bitstring be a long[].
    private static byte readBitsFromByteArrayInLogicalOrder(byte[] container, int offset, int len) {
        Validate.isTrue(len <= 8);
        Validate.isTrue(offset + len <= container.length * 8);
        
        int byteOffset = offset / 8;
        int bitOffset = offset % 8;
        
        int idx = byteOffset;
        
        int lenOfBitsRemainingInByte = 8 - bitOffset;
        
        byte ret;
        if (len <= lenOfBitsRemainingInByte) {
            ret = (byte) (isolateBitsToBottom(container[idx], bitOffset, len) & 0xFFL);
        } else {
            long byte1 = container[idx] & 0xFFL;
            long byte2 = container[idx + 1] & 0xFFL;
            int byte1BitOffset = bitOffset;
            int byte1BitLen = Math.min(8 - bitOffset, len);
            int byte2BitOffset = 0;
            int byte2BitLen = len - byte1BitLen;
            
            long portion1 = isolateBitsToBottom(byte1, byte1BitOffset, byte1BitLen);
            long portion2 = isolateBitsToBottom(byte2, byte2BitOffset, byte2BitLen);
            
            long combined = (portion1 << byte2BitLen) | portion2;
            
            ret = (byte) (combined & 0xFF);
        }
        
        return ret;
    }
    
    // Why work on longs? It may be more efficient to have the byte[] containing the bitstring be a long[].
    private static byte readBitsFromByteArrayInReadOrder(byte[] container, int offset, int len) {
        Validate.isTrue(len <= 8);
        Validate.isTrue(offset + len <= container.length * 8);
        
        int byteOffset = offset / 8;
        int bitOffset = offset % 8;
        
        int idx = byteOffset;
        
        int lenOfBitsRemainingInByte = 8 - bitOffset;
        
        byte ret;
        if (len <= lenOfBitsRemainingInByte) {
            long byte1 = Long.reverse(container[idx] & 0xFFL) >>> 56;
            ret = (byte) (isolateBitsToBottom(byte1, bitOffset, len) & 0xFFL);
        } else {
            long byte1 = Long.reverse(container[idx] & 0xFFL) >>> 56;
            long byte2 = Long.reverse(container[idx + 1] & 0xFFL) >>> 56;
            int byte1BitOffset = bitOffset;
            int byte1BitLen = Math.min(8 - bitOffset, len);
            int byte2BitOffset = 0;
            int byte2BitLen = len - byte1BitLen;
            
            long portion1 = isolateBitsToBottom(byte1, byte1BitOffset, byte1BitLen);
            long portion2 = isolateBitsToBottom(byte2, byte2BitOffset, byte2BitLen);
            
            long combined = (portion1 << byte2BitLen) | portion2;
            
            ret = (byte) (combined & 0xFF);
        }
        
        return ret;
    }

    // Why work on longs? It may be more efficient to have the byte[] containing the bitstring be a long[].
    private static long isolateBitsToBottom(long data, int offset, int len) {
        Validate.isTrue(offset >= 0);
        Validate.isTrue(len >= 0);
        Validate.isTrue(offset + len <= 64);
        
        long mask = (1L << len) - 1L;
        
        return (data >>> offset) & mask;
    }
    
    private static int calculateRequiredByteArraySize(int bitLength) {
        Validate.inclusiveBetween(0, Integer.MAX_VALUE, bitLength);
        
        int fullByteCount = bitLength / 8;
        int remainingBits = bitLength % 8;
        
        int byteLength = fullByteCount + (remainingBits == 0 ? 0 : 1);
        
        return byteLength;
    }
    
    /**
     * Get the number of bits that are that this bitstring's prefix shares with another bitstring.
     * @param other other bitstring to test against
     * @return number of common prefix bits
     * @throws NullPointerException if any argument is {@code null}
     */
    public int getSharedPrefixLength(BitString other) {
        Validate.notNull(other);
        
        int maxCompareLenAsBits = Math.min(bitLength, other.bitLength);
        int maxCompareLenAsBytes = calculateRequiredByteArraySize(maxCompareLenAsBits);
        
        
        int nextByteIdx = 0;
        for (int i = 0; i < maxCompareLenAsBytes; i++) {
            if (other.data[i] != data[i]) {
                break;
            }
            nextByteIdx++;
        }
        
        if (nextByteIdx == maxCompareLenAsBytes) {
            // All bytes matched, both string prefixes match entirely
            return maxCompareLenAsBits;
        }
        
        int thisLastByte = data[nextByteIdx] & 0xFF;
        int otherLastByte = other.data[nextByteIdx] & 0xFF;
      
        int bitMatchCount = 0;
        int remainingBits = maxCompareLenAsBits - (nextByteIdx * 8);
        for (int i = 0; i < remainingBits; i++) {
            int thisBit = (thisLastByte >> i) & 0x01;
            int otherBit = (otherLastByte >> i) & 0x01;
            if (thisBit != otherBit) {
                break;
            }
            bitMatchCount++;
        }
        
        int finalBitMatchCount = (nextByteIdx * 8) + bitMatchCount;
        
        return finalBitMatchCount;
    }
    
    /**
     * Get the number of bits that are that this bitstring's suffix shares with another bitstring.
     * @param other other bitstring to test against
     * @return number of common suffix bits
     * @throws NullPointerException if any argument is {@code null}
     */
    public int getSharedSuffixLength(BitString other) {
        Validate.notNull(other);
        
        // You can make this more efficient by doing something similar to getSharedPrefixLength()
        
        int thisBitOffset = bitLength - 1;
        int otherBitOffset = other.bitLength - 1;
        int bitMatchCount = 0;
        while (thisBitOffset >= 0 && otherBitOffset >= 0) {
            boolean thisBit = getBit(thisBitOffset);
            boolean otherBit = other.getBit(otherBitOffset);
            if (thisBit != otherBit) {
                break;
            }
            bitMatchCount++;
            thisBitOffset--;
            otherBitOffset--;
        }
        
        return bitMatchCount;
    }
    
    /**
     * Get bit from this bitstring.
     * @param offset offset of bit
     * @return {@code true} if bit is 1, {@code false} if bit is 0
     * @throws IllegalArgumentException if {@code offset < 0} or if {@code offset > bitLength}
     */
    public boolean getBit(int offset) {
        Validate.isTrue(offset >= 0);
        Validate.isTrue(offset < bitLength);
        
        int bitPos = offset % 8;
        int bytePos = offset / 8;
        
        int bitMask = 1 << bitPos;
        return (data[bytePos] & bitMask) != 0;
    }

    /**
     * Set bit within a copy of this bitstring.
     * @param offset offset of bit
     * @param bit {@code true} if bit is 1, {@code false} if bit is 0
     * @return new ID that has bit set
     * @throws IllegalArgumentException if {@code offset < 0} or if {@code offset > bitLength}
     */
    public BitString setBit(int offset, boolean bit) {
        Validate.isTrue(offset >= 0);
        Validate.isTrue(offset < bitLength);
        
        byte[] dataCopy = Arrays.copyOf(data, data.length);
        
        int bitPos = offset % 8;
        int bytePos = offset / 8;
        
        if (bit) {
            int bitMask = 1 << bitPos;
            dataCopy[bytePos] |= bitMask;
        } else {
            int bitMask = ~(1 << bitPos);
            dataCopy[bytePos] &= bitMask;
        }
        
        return new BitString(dataCopy, bitLength);
    }

    /**
     * Flip bit within a copy of this bitstring.
     * @param offset offset of bit
     * @return new bitstring that has bit flipped
     * @throws IllegalArgumentException if {@code offset < 0} or if {@code offset > bitLength}
     */
    public BitString flipBit(int offset) {
        Validate.isTrue(offset >= 0);
        Validate.isTrue(offset < bitLength);
        
        boolean bit = getBit(offset);
        return setBit(offset, !bit);
    }

    /**
     * Get multiple bits from this bitstring.
     * @param offset offset of bit within this bitstring to read from
     * @param len number of bits to get
     * @return bits starting from {@code offset} to {@code offset + len} from this bitstring
     * @throws IllegalArgumentException if {@code offset < 0} or if {@code offset > bitLength} or
     * {@code offset + other.bitLength > bitLength}
     */
    public BitString getBits(int offset, int len) {
        Validate.isTrue(offset >= 0);
        Validate.isTrue(offset <= this.bitLength);
        int end = offset + len;
        Validate.isTrue(end <= this.bitLength);
        
        int lenAsBytes = calculateRequiredByteArraySize(len);
        byte[] dataCopy = new byte[lenAsBytes];
        
        // TODO: You can make this much more efficient
        for (int i = 0; i < len; i++) {
            int readIdx = offset + i;
            int readBitPos = readIdx % 8;
            int readBytePos = readIdx / 8;

            int readBitMask = 1 << readBitPos;
            boolean bit = (data[readBytePos] & readBitMask) != 0;
            
            int writeIdx = i;
            int writeBitPos = writeIdx % 8;
            int writeBytePos = writeIdx / 8;
            
            if (bit) {
                int bitMask = 1 << writeBitPos;
                dataCopy[writeBytePos] |= bitMask;
            } else {
                int bitMask = ~(1 << writeBitPos);
                dataCopy[writeBytePos] &= bitMask;
            }
        }
        
        return new BitString(dataCopy, len);
    }

    /**
     * Get multiple bits from this bitstring as a long.
     * <p>
     * For example {@code BitString.createFromNumber(0x3CFA000000000000L, 0, 16)} will generate the bit string {@code 0011 1100 0101 1111},
     * which if you called {@code getBitsAsLong(8, 4)} on would generate the long {@code 5L} ({@code 5L = 0x0101L}).
     * @param offset offset of bit within this bitstring to read from
     * @param len number of bits to get
     * @return bits starting from {@code offset} to {@code offset + len} from this bitstring, inside of a long
     * @throws IllegalArgumentException if {@code offset < 0} or if {@code offset > bitLength} or if {@code offset + len > bitLength} or if
     * {@code len > 64}
     */
    public long getBitsAsLong(int offset, int len) {
        Validate.isTrue(offset >= 0);
        Validate.isTrue(offset <= this.bitLength);
        int end = offset + len;
        Validate.isTrue(end <= this.bitLength);
        Validate.isTrue(len <= 64);
        
        long dataCopy = 0;
        
        // TODO: You can make this much more efficient
        for (int i = 0; i < len; i++) {
            int readIdx = offset + i;
            int readBitPos = readIdx % 8;
            int readBytePos = readIdx / 8;

            int readBitMask = 1 << readBitPos;
            boolean bit = (data[readBytePos] & readBitMask) != 0;
            
            int writeBitPos = (len - i) - 1;
            
            if (bit) {
                long bitMask = 1L << writeBitPos;
                dataCopy |= bitMask;
            } else {
                long bitMask = ~(1L << writeBitPos);
                dataCopy &= bitMask;
            }
        }
        
        return dataCopy;
    }
    
    /**
     * Set multiple bits within a copy of this bitstring.
     * @param offset offset of bit within this bitstring to write to
     * @param other bits to set
     * @return new bitstring that has bit set
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if {@code offset < 0} or if {@code offset > bitLength} or
     * {@code offset + other.bitLength > bitLength}
     */
    public BitString setBits(int offset, BitString other) {
        Validate.notNull(other);
        Validate.isTrue(offset >= 0);
        Validate.isTrue(offset <= bitLength);
        int end = offset + other.bitLength;
        Validate.isTrue(end <= bitLength);
        
        byte[] dataCopy = Arrays.copyOf(data, data.length);
        
        // TODO: You can make this much more efficient
        for (int i = offset; i < end; i++)  {
            int bitPos = i % 8;
            int bytePos = i / 8;

            if (other.getBit(i - offset)) {
                int bitMask = 1 << bitPos;
                dataCopy[bytePos] |= bitMask;
            } else {
                int bitMask = ~(1 << bitPos);
                dataCopy[bytePos] &= bitMask;
            }
        }
        
        return new BitString(dataCopy, bitLength);
    }
    
    /**
     * Append bits to a copy of this bitstring.
     * @param other bits to append
     * @return new bitstring that has bit set
     * @throws NullPointerException if any argument is {@code null}
     */
    public BitString appendBits(BitString other) {
        Validate.notNull(other);
        
        int offset = bitLength;
        int end = offset + other.bitLength;
        
        int arrLen = calculateRequiredByteArraySize(end);
        
        byte[] dataCopy = Arrays.copyOf(data, arrLen);
        
        // TODO: You can make this much more efficient
        for (int i = offset; i < end; i++)  {
            int bitPos = i % 8;
            int bytePos = i / 8;

            if (other.getBit(i - offset)) {
                int bitMask = 1 << bitPos;
                dataCopy[bytePos] |= bitMask;
            } else {
                int bitMask = ~(1 << bitPos);
                dataCopy[bytePos] &= bitMask;
            }
        }
        
        return new BitString(dataCopy, end);
    }

    /**
     * Gets the maximum bit length for this bitstring.
     * @return max bit length for bitstring
     */
    public int getBitLength() {
        return bitLength;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Arrays.hashCode(this.data);
        hash = 89 * hash + this.bitLength;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final BitString other = (BitString) obj;
        if (!Arrays.equals(this.data, other.data)) {
            return false;
        }
        if (this.bitLength != other.bitLength) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(bitLength + 13); //13 = '(' + max characters an int can be when converted to string + ')' + ' '
//        sb.append('(').append(bitLength).append(") ");
        for (int i = 0; i < bitLength; i++) {
//            if (i % 4 == 0 && i != 0) {
//                sb.append(' ');
//            }
            sb.append(getBit(i) /*== true*/ ? 1 : 0);
        }
        return sb.toString();
    }
}