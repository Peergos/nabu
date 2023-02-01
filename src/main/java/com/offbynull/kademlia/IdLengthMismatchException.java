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

import org.apache.commons.lang3.Validate;

/**
 * Thrown to indicate that the bit length of input ID doesn't match (is too large or too small).
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class IdLengthMismatchException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    private final Id id;
    private final int expectedLength;

    IdLengthMismatchException(Id id, int expectedBitLength) {
        super("ID bitlength size mismatch (required " + expectedBitLength +  "): " + id + ")");
        Validate.notNull(id);
        Validate.isTrue(expectedBitLength > 0); // ids will always be 1 bit or greater
        Validate.isTrue(expectedBitLength != id.getBitLength()); // what's the point of throwing an exception for the bitlengths
                                                                            // not matching if the bitlengths match?
        this.id = id;
        this.expectedLength = expectedBitLength;
    }

    /**
     * Get ID of the input node.
     * @return ID of input node
     */
    public Id getId() {
        return id;
    }

    /**
     * Get the length of the ID of the Kademlia node being operated on.
     * @return length of the ID of the Kademlia node being operated on
     */
    public int getExpectedLength() {
        return expectedLength;
    }

}
