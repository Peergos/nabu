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
 * K-bucket parameters.
 * @author Kasra Faghihi
 */
public final class KBucketParameters {

    private final int bucketSize;
    private final int cacheSize;

    /**
     * Construct a {@link KBucketParameters} object.
     *
     * @param bucketSize maximum number of nodes k-bucket can hold
     * @param cacheSize maximum number of cache nodes k-bucket can hold
     * @throws IllegalArgumentException if any numeric argument is negative
     */
    public KBucketParameters(int bucketSize, int cacheSize) {
        Validate.isTrue(bucketSize >= 0);
        Validate.isTrue(cacheSize >= 0);
        this.bucketSize = bucketSize;
        this.cacheSize = cacheSize;
    }

    int getBucketSize() {
        return bucketSize;
    }

    int getCacheSize() {
        return cacheSize;
    }
}