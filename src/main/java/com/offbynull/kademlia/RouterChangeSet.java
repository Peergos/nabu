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
 * Changes resulting from some operation performed on a {@link Router}.
 * @author Kasra Faghihi
 */
public final class RouterChangeSet {
    private final RouteTreeChangeSet routeTreeChangeSet;

    RouterChangeSet(RouteTreeChangeSet routeTreeChangeSet) {
        Validate.notNull(routeTreeChangeSet);
        this.routeTreeChangeSet = routeTreeChangeSet;
    }

    /**
     * Get the changes resulting from the operation performed on the internal {@link RouteTree}.
     * @return changes resulting from the operation performed on the internal route tree
     */
    public RouteTreeChangeSet getRouteTreeChangeSet() {
        return routeTreeChangeSet;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + Objects.hashCode(this.routeTreeChangeSet);
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
        final RouterChangeSet other = (RouterChangeSet) obj;
        if (!Objects.equals(this.routeTreeChangeSet, other.routeTreeChangeSet)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RouterChangeSet{" + "routeTreeChangeSet=" + routeTreeChangeSet + '}';
    }

}
