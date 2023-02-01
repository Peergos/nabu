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
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import org.apache.commons.lang3.Validate;

final class NodeMostRecentSet {
    private final Id baseId;
    private final LinkedList<Activity> entries;

    private int maxSize;

    NodeMostRecentSet(Id baseId, int maxSize) {
        Validate.notNull(baseId);
        Validate.isTrue(maxSize >= 0);
        
        this.baseId = baseId;
        this.maxSize = maxSize;

        this.entries = new LinkedList<>();
    }
  
    public ActivityChangeSet touch(Instant time, Node node, boolean allowLinkMismatch) {
        Validate.notNull(time);
        Validate.notNull(node);
        
        Id nodeId = node.getId();
        
        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
//        Validate.isTrue(!nodeId.equals(baseId)); // Don't reject adding self
        
        // TODO: You can make this way more efficient if you used something like MultiTreeSet (guava) and sorted based on entry time

        // Remove existing entry
        Activity oldEntry = null;
        ListIterator<Activity> it = entries.listIterator();
        while (it.hasNext()) {
            Activity entry = it.next();

            Id entryId = entry.getNode().getId();

            if (entryId.equals(nodeId)) {
                if (!allowLinkMismatch) {
                    InternalValidate.matchesLink(entry.getNode(), node);
                }

                // remove
                it.remove();
                oldEntry = entry;
                break;
            }
        }

        
        // Add entry
        Activity newEntry = new Activity(node, time);
        it = entries.listIterator(entries.size());
        boolean added = false;
        while (it.hasPrevious()) {
            Activity entry = it.previous();

            if (entry.getTime().isBefore(time)) {
                it.next(); // move forward 1 space, we want to add to element just after entry
                it.add(newEntry);
                added = true;
                break;
            }
        }

        if (!added) { // special case where newEntry needs to be added at the end of entries, not handled by loop above
            entries.addFirst(newEntry);
        }

        
        // Set has become too large, remove the item with the earliest time
        Activity discardedEntry = null;
        if (entries.size() > maxSize) {
            // if the node removed with the earliest time is the one we just added, then report that node couldn't be added
            discardedEntry = entries.removeFirst();
            if (discardedEntry.equals(newEntry)) {
                return ActivityChangeSet.NO_CHANGE;
            }
        }

        
        // Add successful
        if (oldEntry != null) {
            Validate.validState(discardedEntry == null); // sanity check, must not have discarded anything
            
            // updated existing node
            return ActivityChangeSet.updated(newEntry);
        } else {
            // if block above ensures oldEntry is null if we're in this else block, so sanity check below isn't nessecary
            // Validate.validState(oldEntry == null); // sanity check, node being touched must not have already existed
            
            // added new node
            Collection<Activity> addedEntries = singletonList(newEntry);
            Collection<Activity> removedEntries = discardedEntry == null ? emptyList() : singletonList(discardedEntry);
            Collection<Activity> updatedEntries = emptyList();
            return new ActivityChangeSet(addedEntries, removedEntries, updatedEntries);
        }
    }

    public ActivityChangeSet remove(Node node) {
        Validate.notNull(node);
        
        Id nodeId = node.getId();
        
        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
        
        ListIterator<Activity> it = entries.listIterator();
        while (it.hasNext()) {
            Activity entry = it.next();

            Id entryId = entry.getNode().getId();

            if (entryId.equals(nodeId)) {
                InternalValidate.matchesLink(entry.getNode(), node);

                // remove
                it.remove();
                return ActivityChangeSet.removed(entry);
            }
        }
        
        return ActivityChangeSet.NO_CHANGE;
    }

    public ActivityChangeSet resize(int maxSize) {
        Validate.isTrue(maxSize >= 0);
        
        int discardCount = this.maxSize - maxSize;
        
        List<Activity> removed = new LinkedList<>();
        for (int i = 0; i < discardCount; i++) {
            Activity removedEntry = entries.removeFirst(); // remove node that hasn't been touched the longest
            removed.add(removedEntry);
        }
        
        this.maxSize = maxSize;
        
        return ActivityChangeSet.removed(removed);
    }

    public ActivityChangeSet removeMostRecent(int count) {
        LinkedList<Activity> removed = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            Activity e = entries.removeLast();
            if (e == null) {
                break;
            }
            removed.addFirst(e);
        }
        return ActivityChangeSet.removed(removed);
    }
    
    public List<Activity> dump() {
        return new ArrayList<>(entries);
    }
    
    public Instant lastestActivityTime() { // time of the latest entry in this set, or null if set is empty
        if (entries.isEmpty()) {
            return null;
        }
        
        return entries.getLast().getTime();
    }

    public int size() {
        return entries.size();
    }

    public int maxSize() {
        return maxSize;
    }

    @Override
    public String toString() {
        return "NodeMostRecentSet{" + "baseId=" + baseId + ", entries=" + entries + ", maxSize=" + maxSize + '}';
    }




}
