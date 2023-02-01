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
import java.util.List;
import org.apache.commons.lang3.Validate;

/**
 * Encapsulates a {@link RouteTree}. May provide additional functionality in the future.
 * @author Kasra Faghihi
 */
public final class Router {
    // NOTE: Right now this class just encapsulates a RouteTree, but it may be refactored in the future to support more elaborate logic
    // (e.g. keeping track of the number of times a node didn't respond in time and locking that node/marking it stale)
    private final Id baseId;
    private final RouteTree routeTree;
    
    private Instant lastTouchTime;

    /**
     * Constructs a {@link Router} object.
     * @param baseId ID of the node that this router is for
     * @param branchStrategy branching strategy for the route tree to be created by this router (dictates how many branches to create at
     * each depth)
     * @param bucketStrategy bucket strategy for the route tree to be created by this router (dictates k-bucket parameters for each
     * k-bucket)
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalStateException if either {@code branchStrategy} or {@code bucketStrategy} generates invalid data (see interfaces for
     * restrictions)
     */
    public Router(Id baseId,
            RouteTreeBranchStrategy branchStrategy,
            RouteTreeBucketStrategy bucketStrategy) {
        Validate.notNull(baseId);
        Validate.notNull(branchStrategy);
        Validate.notNull(bucketStrategy);
        
        this.baseId = baseId;
        this.routeTree = new RouteTree(baseId, branchStrategy, bucketStrategy);
        this.lastTouchTime = Instant.MIN;
    }

    /**
     * Constructs a {@link Router} object where the route tree created by this router uses a {@link SimpleRouteTreeStrategy} for the
     * branching strategy and the bucket strategy.
     * @param baseId ID of the node that this router is for
     * @param branchesPerLevel number of branches to generate whenever a k-bucket splits
     * @param maxNodesPerBucket maximum number of nodes allowed in each k-bucket
     * @param maxCacheNodesPerBucket maximum number of cache nodes allowed in each k-bucket
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if any numeric argument is {@code 0} or less, or if
     * {@code branchesPerLevel < 2 || !isPowerOfTwo(branchesPerLevel)}, or if {@code baseId.getBitLength() % branchesPerLevel != 0} (if the
     * number of branches per level doesn't divide evenly in to bit length, the routing tree will have too many branches at the last level)
     */
    public Router(Id baseId, int branchesPerLevel, int maxNodesPerBucket, int maxCacheNodesPerBucket) {
        this(baseId,
                new SimpleRouteTreeStrategy(baseId, branchesPerLevel, maxNodesPerBucket, maxCacheNodesPerBucket),
                new SimpleRouteTreeStrategy(baseId, branchesPerLevel, maxNodesPerBucket, maxCacheNodesPerBucket));
    }
    
    /**
     * Updates the appropriate k-bucket in the route tree associated with this router router by touching it. When the Kademlia node that
     * this router is for receives a request or response from some other node in the network, this method should be called.
     * <p>
     * See {@link KBucket#touch(Instant, Node) } for more information.
     * @param time time which request or response came in
     * @param node node which issued the request or response
     * @return changes to collection of stored nodes and replacement cache of the k-bucket effected
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this router is for)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this router is for)
     * @throws BackwardTimeException if {@code time} is less than the time used in the previous invocation of this method
     * @throws LinkMismatchException if this router already contains a node with {@code node}'s ID but with a different link (SPECIAL
     * CASE: If the contained node is marked as stale, this exception will not be thrown. Since the node is marked as stale, it means it
     * should have been replaced but the replacement cache was empty. As such, this case is treated as if this were a new node replacing
     * a stale node, not a stale node being reverted to normal status -- the fact that the IDs are the same but the links don't match
     * doesn't matter)
     * @see KBucket#touch(Instant, Node)
     */
    public RouterChangeSet touch(Instant time, Node node) {
        Validate.notNull(time);
        Validate.notNull(node);
        
        InternalValidate.forwardTime(lastTouchTime, time); // time must be >= lastUpdatedTime
        this.lastTouchTime = time;
        
        Id nodeId = node.getId();
        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
        InternalValidate.notMatchesBase(baseId, nodeId);

        
        
        // Touch routing tree
        RouteTreeChangeSet routeTreeChangeSet = routeTree.touch(time, node);

        
        return new RouterChangeSet(routeTreeChangeSet);
    }
    
    /**
     * Get all k-bucket prefixes in the route tree associated with this router.
     * @return all k-bucket prefixes in the route tree associated with this router
     */
    public List<BitString> dumpBucketPrefixes() {
        return routeTree.dumpBucketPrefixes();
    }
    
    /**
     * Searches the route tree associated with this router for the closest nodes to some ID. Node closeness is determined by the XOR metric
     * -- Kademlia's notion of distance.
     * <p>
     * Note this method will never return yourself (the node that this routing table is for).
     * @param id ID to search for
     * @param max maximum number of results to give back
     * @param includeStale if {@code true}, includes stale nodes in the results
     * @return up to {@code max} closest nodes to {@code id} (less are returned if this route table contains less than {@code max} nodes)
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if any numeric argument is negative
     * @throws IdLengthMismatchException if the bitlength of {@code id} doesn't match the bitlength of the ID that this router is for
     * (the ID of the node this router belongs to)
     */
    public List<Node> find(Id id, int max, boolean includeStale) {
        Validate.notNull(id);
        Validate.isTrue(max >= 0); // why would anyone want 0 items returned? let thru anyways
        
        InternalValidate.matchesLength(baseId.getBitLength(), id);
        // do not stop from finding self (base) -- you may want to update closest
        
        List<Activity> closestNodesInRoutingTree = routeTree.find(id, max, includeStale);
        
        ArrayList<Node> res = new ArrayList<>(closestNodesInRoutingTree.size());
        closestNodesInRoutingTree.stream()
                .map(x -> x.getNode())
                .forEachOrdered(res::add);
        
        return res;
    }
    
    /**
     * Marks a node within the route tree associated with this router as stale (meaning that you're no longer able to communicate with it),
     * evicting it and replacing it with the most recent node in the effected k-bucket's replacement cache. 
     * <p>
     * See {@link KBucket#stale(Node) } for more information.
     * @param node node to mark as stale
     * @return changes to collection of stored nodes and replacement cache of the k-bucket effected
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this router is for)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this router is for)
     * @throws NodeNotFoundException if this router doesn't contain {@code node}
     * @throws LinkMismatchException if this router contains a node with {@code node}'s ID but with a different link
     * @throws BadNodeStateException if this router contains {@code node} but {@code node} is marked as locked
     * @see KBucket#stale(Node)
     */
    public RouterChangeSet stale(Node node) {
        Validate.notNull(node);
        
        Id nodeId = node.getId();
        
        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
        InternalValidate.notMatchesBase(baseId, nodeId); 
        
        RouteTreeChangeSet routeTreeChangeSet = routeTree.stale(node);
        
        return new RouterChangeSet(routeTreeChangeSet);
    }
    
    // lock means "avoid contact" AKA avoid returning on "find" until unlocked. unlocking only happens on unlock(), not on touch()...
    //
    // according to kademlia...
    //
    // A related problem is that because Kademlia uses UDP, valid contacts will sometimes fail to respond when network packets are dropped.
    // Since packet loss often indicates network congestion, Kademlia locks unresponsive contacts and avoids sending them any further RPCs
    // for an exponentially increasing backoff interval.
    //
    // that means because we're in a "backoff period", even if we get touch()'d by that node, we still want to keep it unfindable/locked...
    // up until the point that we explictly decide to to make it findable/unlocked.
//
//COMMENTED OUT METHODS BELOW BECAUSE THEY DONT RETURN A ROUTERTREECHANGSET AND ARENT CURRENTLY BEING USED FOR ANYTHING. IF YOU'RE GOING TO
//RE-ENABLE, HAVE ROUTER DECIDE WHAT SHOULD BE LOCKED/STALE BASED ON AN UNRESPONSIVE COUNTER THAT GETS HIT WHENEVER SOMETHING IS
//UNRESPONSIVE
//    public void lock(Node node) {
//        Validate.notNull(node);
//        
//        Id nodeId = node.getId();
//        
//        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
//        InternalValidate.notMatchesBase(baseId, nodeId); 
//        
//        routeTree.lock(node); // will throw exc if node not in routetree
//    }
//
//    public void unlock(Node node) {
//        Validate.notNull(node);
//        
//        Id nodeId = node.getId();
//        
//        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
//        InternalValidate.notMatchesBase(baseId, nodeId); 
//        
//        routeTree.unlock(node); // will throw exc if node not in routetree
//    }
}
