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

import java.util.Objects;
import org.apache.commons.lang3.Validate;

/**
 * Changes resulting from some operation performed on a {@link RouteTree}.
 * @author Kasra Faghihi
 */
public final class RouteTreeChangeSet {
    private final BitString kBucketPrefix;
    private final KBucketChangeSet kBucketChangeSet;

    RouteTreeChangeSet(BitString kBucketPrefix, KBucketChangeSet kBucketChangeSet) {
        Validate.notNull(kBucketPrefix);
        Validate.notNull(kBucketChangeSet);

        this.kBucketPrefix = kBucketPrefix;
        this.kBucketChangeSet = kBucketChangeSet;
    }

    /**
     * Get prefix of the k-bucket changed within this routing tree.
     * @return prefix of the k-bucket changed within this routing tree
     */
    public BitString getKBucketPrefix() {
        return kBucketPrefix;
    }

    /**
     * Get changes performed on the k-bucket within this routing tree.
     * @return changes performed on the k-bucket within this routing tree
     */
    public KBucketChangeSet getKBucketChangeSet() {
        return kBucketChangeSet;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 43 * hash + Objects.hashCode(this.kBucketPrefix);
        hash = 43 * hash + Objects.hashCode(this.kBucketChangeSet);
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
        final RouteTreeChangeSet other = (RouteTreeChangeSet) obj;
        if (!Objects.equals(this.kBucketPrefix, other.kBucketPrefix)) {
            return false;
        }
        if (!Objects.equals(this.kBucketChangeSet, other.kBucketChangeSet)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RouteTreeChangeSet{" + "kBucketPrefix=" + kBucketPrefix + ", kBucketChangeSet=" + kBucketChangeSet + '}';
    }
    
}
