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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.collections4.map.MultiValueMap;
import org.apache.commons.lang3.Validate;

final class TimeSet<V> {
    private final HashMap<V, Instant> valueLookup;
    private final TreeMap<Instant, HashSet<V>> timeLookup;
    private final MultiValueMap<Instant, V> timeLookupDecorator;

    TimeSet() {
        valueLookup = new HashMap<>();
        timeLookup = new TreeMap<>();
        timeLookupDecorator = MultiValueMap.multiValueMap(timeLookup, () -> new HashSet<>());
    }

    public void remove(V value) {
        Validate.notNull(value);
        
        Instant time = valueLookup.remove(value);
        if (time == null) {
            return;
        }
        
        timeLookupDecorator.removeMapping(time, value);
    }

    public void insert(Instant time, V value) {
        Validate.notNull(time);
        Validate.notNull(value);
        
        Instant existing = valueLookup.putIfAbsent(value, time);
        Validate.isTrue(existing == null); // should not allow putting in a value that already exists
        
        timeLookupDecorator.put(time, value);
    }
    
    public List<V> getBefore(Instant time, boolean inclusive) {
        Validate.notNull(time);
        
        Map<Instant, HashSet<V>> subMap = timeLookup.headMap(time, inclusive);
        
        LinkedList<V> ret = new LinkedList<>();
        subMap.entrySet().stream()
                .flatMap(x -> x.getValue().stream())
                .forEachOrdered(ret::add);
        
        return new ArrayList<>(ret);
    }

    public List<V> getAfter(Instant time, boolean inclusive) {
        Validate.notNull(time);
        
        Map<Instant, HashSet<V>> subMap = timeLookup.tailMap(time, inclusive);
        
        LinkedList<V> ret = new LinkedList<>();
        subMap.entrySet().stream()
                .flatMap(x -> x.getValue().stream())
                .forEachOrdered(ret::add);
        
        return new ArrayList<>(ret);
    }
    
}
