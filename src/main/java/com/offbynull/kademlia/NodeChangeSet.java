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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.collections4.list.UnmodifiableList;
import org.apache.commons.lang3.Validate;

/**
 * Changes resulting from some operation performed on a collection that stores {@link Node}s (nodes that were added, removed, and updated).
 * <p>
 * Note that a single node can only be added, removed, or updated. It can never be a combination.
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class NodeChangeSet {
    static final NodeChangeSet NO_CHANGE = new NodeChangeSet(emptyList(), emptyList(), emptyList());
    
    private final UnmodifiableList<Node> removed;
    private final UnmodifiableList<Node> added;
    private final UnmodifiableList<Node> updated;
    
    static NodeChangeSet added(Node ... nodes) {
        Validate.notNull(nodes);
        Validate.noNullElements(nodes);
        return added(Arrays.asList(nodes));
    }

    static NodeChangeSet added(Collection<Node> nodes) {
        Validate.notNull(nodes);
        Validate.noNullElements(nodes);
        return new NodeChangeSet(nodes, emptyList(), emptyList());
    }

    static NodeChangeSet removed(Node ... nodes) {
        Validate.notNull(nodes);
        Validate.noNullElements(nodes);
        return removed(Arrays.asList(nodes));
    }

    static NodeChangeSet removed(Collection<Node> nodes) {
        Validate.notNull(nodes);
        Validate.noNullElements(nodes);
        return new NodeChangeSet(emptyList(), nodes, emptyList());
    }

    static NodeChangeSet updated(Node ... nodes) {
        Validate.notNull(nodes);
        Validate.noNullElements(nodes);
        return updated(Arrays.asList(nodes));
    }

    static NodeChangeSet updated(Collection<Node> nodes) {
        Validate.notNull(nodes);
        Validate.noNullElements(nodes);
        return new NodeChangeSet(emptyList(), emptyList(), nodes);
    }
    
    NodeChangeSet(Collection<Node> added, Collection<Node> removed, Collection<Node> updated) {
        Validate.notNull(removed);
        Validate.notNull(added);
        Validate.notNull(updated);
        Validate.noNullElements(removed);
        Validate.noNullElements(added);
        Validate.noNullElements(updated);
        
        // ensure that there aren't any duplicate ids
        Set<Id> tempSet = new HashSet<>();
        removed.stream().map(x -> x.getId()).forEach(x -> tempSet.add(x));
        added.stream().map(x -> x.getId()).forEach(x -> tempSet.add(x));
        updated.stream().map(x -> x.getId()).forEach(x -> tempSet.add(x));
        Validate.isTrue(tempSet.size() == added.size() + removed.size() + updated.size());
        
        this.removed = (UnmodifiableList<Node>) UnmodifiableList.unmodifiableList(new ArrayList<>(removed));
        this.added = (UnmodifiableList<Node>) UnmodifiableList.unmodifiableList(new ArrayList<>(added));
        this.updated = (UnmodifiableList<Node>) UnmodifiableList.unmodifiableList(new ArrayList<>(updated));
    }

    /**
     * Get the list of nodes removed.
     * @return list (unmodifiable) of nodes removed
     */
    public UnmodifiableList<Node> viewRemoved() {
        return removed;
    }

    /**
     * Get the list of nodes added.
     * @return list (unmodifiable) of nodes added
     */
    public UnmodifiableList<Node> viewAdded() {
        return added;
    }

    /**
     * Get the list of nodes updated.
     * @return list (unmodifiable) of nodes updated
     */
    public UnmodifiableList<Node> viewUpdated() {
        return updated;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 23 * hash + Objects.hashCode(this.removed);
        hash = 23 * hash + Objects.hashCode(this.added);
        hash = 23 * hash + Objects.hashCode(this.updated);
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
        final NodeChangeSet other = (NodeChangeSet) obj;
        if (!Objects.equals(this.removed, other.removed)) {
            return false;
        }
        if (!Objects.equals(this.added, other.added)) {
            return false;
        }
        if (!Objects.equals(this.updated, other.updated)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "NodeChangeSet{" + "removed=" + removed + ", added=" + added + ", updated=" + updated + '}';
    }

}
