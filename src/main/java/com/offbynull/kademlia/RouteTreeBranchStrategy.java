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


/**
 * Branching strategy for a Kademlia routing tree.
 * @author Kasra Faghihi
 */
public interface RouteTreeBranchStrategy {
    /**
     * Get the number of child branches to create at some branch in the routing tree. This method is called when a k-bucket within a routing
     * tree is to be split. It identifies the number of k-buckets to split the original k-bucket in to. For example, imagine the following
     * routing tree...
     * <pre>
     *       0/\1
     * </pre>
     * If this method were to return returned 2 for the prefix {@code 0}, the tree would look like this...
     * <pre>
     *       0/\1
     *      /  
     *    0/\1
     * </pre>
     * @param prefix prefix of the branch in the routing tree with the k-bucket being split
     * @return number of child branches to create -- MUST BE &gt;= 2 AND MUST BE A POWER OF 2 (e.g. 2, 4, 8, etc..)
     * @throws NullPointerException if any argument is {@code null}
     */
    int getBranchCount(BitString prefix);
}
