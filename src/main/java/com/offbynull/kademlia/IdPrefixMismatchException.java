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
 * Thrown to indicate that the prefix of the input ID doesn't match.
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class IdPrefixMismatchException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    private final Id id;
    private final BitString expectedPrefix;

    IdPrefixMismatchException(Id id, BitString expectedPrefix) {
        super("ID prefix mismatch (required " + expectedPrefix +  "): " + id + ")");
        Validate.notNull(id);
        Validate.notNull(expectedPrefix);
        
        // what's the point of throwing an exception for not having a shared prefix if you have a shared prefix?
        Validate.isTrue(id.getBitString().getSharedPrefixLength(expectedPrefix) != expectedPrefix.getBitLength());
        this.id = id;
        this.expectedPrefix = expectedPrefix;
    }

    /**
     * Get the input ID.
     * @return input ID
     */
    public Id getId() {
        return id;
    }

    /**
     * Get the expected prefix.
     * @return expected prefix
     */
    public BitString getExpectedPrefix() {
        return expectedPrefix;
    }


}
