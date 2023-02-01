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
 * Supplies simplistic route tree branching and k-bucket generation strategy.
 * <p>
 * This class is immutable.
 * @author Kasra Faghihi
 */
public final class SimpleRouteTreeStrategy implements RouteTreeBranchStrategy,
        RouteTreeBucketStrategy {
    private final Id baseId;
    private final int branchesPerLevel;
    private final int nodesPerBucket;
    private final int cacheNodesPerBucket;

    /**
     * Constructs a {@link SimpleRouteTreeStrategy} object.
     * @param baseId ID of Kademlia node this supplier is generating a route tree for
     * @param branchesPerLevel number of branches to generate whenever a k-bucket splits
     * @param nodesPerBucket maximum number of nodes allowed in each k-bucket
     * @param cacheNodesPerBucket maximum number of cache nodes allowed in each k-bucket
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if any numeric argument is {@code 0} or less, or if
     * {@code branchesPerLevel < 2 || !isPowerOfTwo(branchesPerLevel)}, or if {@code baseId.getBitLength() % branchesPerLevel != 0} (if the
     * number of branches per level doesn't divide evenly in to bit length, the routing tree will have too many branches at the last level)
     */
    public SimpleRouteTreeStrategy(Id baseId, int branchesPerLevel, int nodesPerBucket, int cacheNodesPerBucket) {
        Validate.notNull(baseId);
        Validate.isTrue(branchesPerLevel >= 2);
        Validate.isTrue(nodesPerBucket > 0);
        Validate.isTrue(cacheNodesPerBucket > 0);
        
        // check to make sure power of 2
        // other ways: http://javarevisited.blogspot.ca/2013/05/how-to-check-if-integer-number-is-power-of-two-example.html
        Validate.isTrue(Integer.bitCount(branchesPerLevel) == 1);
        
        Validate.isTrue(baseId.getBitLength() % branchesPerLevel == 0);
        
        this.baseId = baseId;
        this.branchesPerLevel = branchesPerLevel;
        this.nodesPerBucket = nodesPerBucket;
        this.cacheNodesPerBucket = cacheNodesPerBucket;
    }

    @Override
    public int getBranchCount(BitString prefix) {
        Validate.notNull(prefix);
        
        if (prefix.getBitLength() >= baseId.getBitLength()) {
            // Maximum tree depth reached, cannot branch any further
            return 0;
        }
        
        return branchesPerLevel;
    }

    @Override
    public KBucketParameters getBucketParameters(BitString prefix) {
        Validate.notNull(prefix);
        return new KBucketParameters(nodesPerBucket, cacheNodesPerBucket);
    }
    
}
