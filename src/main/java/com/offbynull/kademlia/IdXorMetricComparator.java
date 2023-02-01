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
import java.util.Comparator;
import org.apache.commons.lang3.Validate;

/**
 * Compares IDs by calculating the distance (Kademlia's notion of distance -- the XOR metric) between the input IDs and a pre-defined ID.
 * <p>
 * Imagine that IDs A and B are being compared by this comparator to see which is closer to ID C (the pre-defined ID). According to the XOR
 * metric...
 * <pre>
 * resAC = XOR(A,C);
 * resBC = XOR(B,C);
 * 
 * if resAC &lt; resAC then A is closer to C
 * if resBC &lt; resAB then B is closer to C
 * if resAB == resBC then A and B are of the same distance from C
 * </pre>
 * So for example...
 * <pre>
 * C=0111
 * A=0001
 * B=0000
 * 
 * resAC = XOR(0001,0111) = 0110
 * resBC = XOR(0000,0111) = 0111
 * </pre>
 * In this case, resAC is closer than resBC because it is less.
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class IdXorMetricComparator implements Comparator<Id>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private final Id baseId;

    /**
     * Constructs a {@link IdXorMetricComparator} object.
     * @param baseId ID to calculate distance against
     * @throws NullPointerException if any argument is {@code null}
     */
    public IdXorMetricComparator(Id baseId) {
        Validate.notNull(baseId);
        this.baseId = baseId;
    }


    // This is the XOR metric...
    //
    // Remember how this works... If you're testing against A and B against C to see which is closer to C...
    //
    // resAC = XOR(A,C);
    // resBC = XOR(B,C);
    //
    // if resAC < resAC then A is closer to C
    // if resBC < resAB then B is closer to C
    // if resAB == resBC then A and B are of the same distance from C
    //
    //
    // Example...
    // C=0111
    // A=0001
    // B=0000
    //
    // resAC = XOR(0001,0111) = 0110
    // resBC = XOR(0000,0111) = 0111
    //
    // In this case, resAC is closer than resBC because it is less.
    //
    //
    // Remember how < works... go compare each single bit from the beginning until you come across a pair of bits that aren't equal (one is
    // 0 and the other is 1). The ID with 0 at that position is less than the other one. So in the example above, after the XORs, offset
    // 3 contains the first differing bit.
    @Override
    public int compare(Id o1, Id o2) {
        Validate.notNull(o1);
        Validate.notNull(o2);
        InternalValidate.matchesLength(baseId.getBitLength(), o1);
        InternalValidate.matchesLength(baseId.getBitLength(), o2);
        
        int bitLen = baseId.getBitLength();
        
        int offset = 0;
        while (offset < bitLen) {
            // read as much as possible, up to 63 bits
            // 63 because the 64th bit will be the sign bit, and we don't want to deal negatives
            int readLen = Math.min(bitLen - offset, 63);
            
            // xor blocks together
            long xorBlock1 = o1.getBitsAsLong(offset, readLen) ^ baseId.getBitsAsLong(offset, readLen);
            long xorBlock2 = o2.getBitsAsLong(offset, readLen) ^ baseId.getBitsAsLong(offset, readLen);
            
            // move offset by the amount we read
            offset += readLen;
            
            // if we read hte full 64 bits, we'd have to use compareUnsigned? we don't want to do that because behind the scenes if creates
            // a BigInteger and does the comparison using that.
            //
            // compare 63 bits together, if not equal, we've found a "greater" one
            int res = Long.compare(xorBlock1, xorBlock2);
            if (res != 0) {
                return res;
            }
        }
        
        // Reaching this point means that o1 and o2 match baseId.
        return 0;
    }
    
}
