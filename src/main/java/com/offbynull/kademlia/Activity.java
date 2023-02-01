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

import java.time.Instant;
import java.util.Objects;
import org.apache.commons.lang3.Validate;

/**
 * Represents the moment in which some action took place on a node (time + node).
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class Activity {
    private final Node node;
    private final Instant time;

    /**
     * Constructs a {@link Activity} object.
     * @param node node
     * @param time time
     * @throws NullPointerException if any argument is {@code null}
     */
    Activity(Node node, Instant time) {
        Validate.notNull(node);
        Validate.notNull(time);
        this.node = node;
        this.time = time;
    }

    /**
     * Get the node action was performed on.
     * @return node
     */
    public Node getNode() {
        return node;
    }

    /**
     * Get the time action was performed.
     * @return time
     */
    public Instant getTime() {
        return time;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 31 * hash + Objects.hashCode(this.node);
        hash = 31 * hash + Objects.hashCode(this.time);
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
        final Activity other = (Activity) obj;
        if (!Objects.equals(this.node, other.node)) {
            return false;
        }
        if (!Objects.equals(this.time, other.time)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Activity{" + "node=" + node + ", time=" + time + '}';
    }
    
}
