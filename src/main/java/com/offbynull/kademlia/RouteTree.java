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
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.Validate;

/**
 * An implementation of Kademlia's route tree. This is an implementation of a <b>strict</b> route tree, meaning that it doesn't perform
 * the irregularly bucket splitting mentioned in the original Kademlia paper. The portion of the paper that deals with irregular bucket
 * splitting was never fully understood (discussion on topic can be found here: http://stackoverflow.com/q/32129978/1196226).
 * <p>
 * A strict route tree is a route tree that is static (doesn't split k-buckets after creation) and extends all the way done to your own
 * ID. For example, the route tree of node 000 would look something like this (assuming that the route tree branches only 1 bit at a
 * time)...
 * <pre>
 *                0/\1
 *                /  [1xx BUCKET]
 *              0/\1
 *              /  [01x BUCKET]
 *            0/\1
 *       [SELF]  [001 BUCKET]
 * </pre>
 * @author Kasra Faghihi
 */
public final class RouteTree {
    private final Id baseId;
    private final RouteTreeNode root;
    private final TimeSet<BitString> bucketUpdateTimes; // prefix to when the prefix's bucket was last updated (not cache)
    
    private Instant lastTouchTime;
    
    /**
     * Construct a {@link RouteTree} object.
     * @param baseId ID of the node that this route tree is for
     * @param branchStrategy branching strategy (dictates how many branches to create at each depth)
     * @param bucketStrategy bucket strategy (dictates k-bucket parameters for each k-bucket)
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalStateException if either {@code branchStrategy} or {@code bucketStrategy} generates invalid data (see interfaces for
     * restrictions)
     */
    public RouteTree(Id baseId, // because id's are always > 0 in size -- it isn't possible for tree creation to mess up
            RouteTreeBranchStrategy branchStrategy,
            RouteTreeBucketStrategy bucketStrategy) {
        Validate.notNull(baseId);
        Validate.notNull(branchStrategy);
        Validate.notNull(bucketStrategy);
        
        this.baseId = baseId; // must be set before creating RouteTreeLevels
        this.bucketUpdateTimes = new TimeSet<>();

        root = createRoot(branchStrategy, bucketStrategy);
        RouteTreeNode child = root;
        while (child != null) {
            child = growParent(child, branchStrategy, bucketStrategy);
        }
        
        // Special case: the routing tree has a bucket for baseId. Nothing can ever access that bucket (calls to
        // touch/stale/find with your own ID will result an exception) and it'll always be empty, so remove it from bucketUpdateTimes.
        bucketUpdateTimes.remove(baseId.getBitString());
        
        this.lastTouchTime = Instant.MIN;
    }

    /**
     * Searches this route tree for the closest nodes to some ID. Node closeness is determined by the XOR metric -- Kademlia's notion of
     * distance.
     * <p>
     * Note this method will never return yourself (the node that this routing table is for).
     * @param id ID to search for
     * @param max maximum number of results to give back
     * @param includeStale if {@code true}, includes stale nodes in the results
     * @return up to {@code max} closest nodes to {@code id} (less are returned if this route table contains less than {@code max} nodes)
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if any numeric argument is negative
     * @throws IdLengthMismatchException if the bitlength of {@code id} doesn't match the bitlength of the ID that this route tree is for
     * (the ID of the node this route tree belongs to)
     */
    public List<Activity> find(Id id, int max, boolean includeStale) {
        Validate.notNull(id);
        InternalValidate.matchesLength(baseId.getBitLength(), id);
//        InternalValidate.notMatchesBase(baseId, id); // commented out because you should be able to search for closest nodes to yourself
        Validate.isTrue(max >= 0); // why would anyone want 0? let thru anyways

        IdXorMetricComparator comparator = new IdXorMetricComparator(id);
        TreeSet<Activity> output = new TreeSet<>((x, y) -> comparator.compare(x.getNode().getId(), y.getNode().getId()));
        
        root.findNodesWithLargestPossiblePrefix(id, output, max, includeStale);
        
        return new ArrayList<>(output);
    }
    
    // used for testing
    List<Activity> dumpBucket(BitString prefix) {
        Validate.notNull(prefix);
        Validate.isTrue(prefix.getBitLength() < baseId.getBitLength()); // cannot be == or >

        KBucket bucket = root.getBucketForPrefix(prefix);        
        return bucket.dumpBucket(true, true, false);
    }
    
    /**
     * Get all k-bucket prefixes in this route tree.
     * @return all k-bucket prefixes in this route tree
     */
    public List<BitString> dumpBucketPrefixes() {
        List<BitString> output = new LinkedList<>();
        root.dumpAllBucketPrefixes(output);
        return new ArrayList<>(output);
    }

    /**
     * Updates the appropriate k-bucket in this route tree by touching it. When the Kademlia node that this route tree is for receives a
     * request or response from some other node in the network, this method should be called.
     * <p>
     * See {@link KBucket#touch(Instant, Node) } for more information.
     * @param time time which request or response came in
     * @param node node which issued the request or response
     * @return changes to collection of stored nodes and replacement cache of the k-bucket effected
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this route tree is for)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this route tree is for)
     * @throws BackwardTimeException if {@code time} is less than the time used in the previous invocation of this method
     * @throws LinkMismatchException if this route tree already contains a node with {@code node}'s ID but with a different link (SPECIAL
     * CASE: If the contained node is marked as stale, this exception will not be thrown. Since the node is marked as stale, it means it
     * should have been replaced but the replacement cache was empty. As such, this case is treated as if this were a new node replacing
     * a stale node, not a stale node being reverted to normal status -- the fact that the IDs are the same but the links don't match
     * doesn't matter)
     * @see KBucket#touch(Instant, Node)
     */
    public RouteTreeChangeSet touch(Instant time, Node node) {
        Validate.notNull(time);
        Validate.notNull(node);
        
        Id id = node.getId();
        InternalValidate.matchesLength(baseId.getBitLength(), id);
        InternalValidate.notMatchesBase(baseId, id);
        
        InternalValidate.forwardTime(lastTouchTime, time); // time must be >= lastUpdatedTime
        lastTouchTime = time;

        KBucket bucket = root.getBucketFor(node.getId()); // because we use this method to find the appropriate kbucket,
                                                          // IdPrefixMismatchException never occurs
        KBucketChangeSet kBucketChangeSet = bucket.touch(time, node);
        BitString kBucketPrefix = bucket.getPrefix();

        // insert last bucket activity time in to bucket update times... it may be null if bucket has never been accessed, in which case
        // we insert MIN instead
        Instant lastBucketActivityTime = bucket.getLatestBucketActivityTime();
        if (lastBucketActivityTime == null) {
            lastBucketActivityTime = Instant.MIN;
        }
        bucketUpdateTimes.remove(kBucketPrefix);
        bucketUpdateTimes.insert(lastBucketActivityTime, kBucketPrefix);

        return new RouteTreeChangeSet(kBucketPrefix, kBucketChangeSet);
    }

    /**
     * Marks a node within this route tree as stale (meaning that you're no longer able to communicate with it), evicting it and replacing
     * it with the most recent node in the effected k-bucket's replacement cache. 
     * <p>
     * See {@link KBucket#stale(Node) } for more information.
     * @param node node to mark as stale
     * @return changes to collection of stored nodes and replacement cache of the k-bucket effected
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this route tree is for)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this route tree is for)
     * @throws NodeNotFoundException if this route tree doesn't contain {@code node}
     * @throws LinkMismatchException if this route tree contains a node with {@code node}'s ID but with a different link
     * @throws BadNodeStateException if this route tree contains {@code node} but {@code node} is marked as locked
     * @see KBucket#stale(Node)
     */
    public RouteTreeChangeSet stale(Node node) {
        Validate.notNull(node);

        Id id = node.getId();
        InternalValidate.matchesLength(baseId.getBitLength(), id);
        InternalValidate.notMatchesBase(baseId, id);
            
        KBucket bucket = root.getBucketFor(node.getId()); // because we use this method to find the appropriate kbucket,
                                                          // IdPrefixMismatchException never occurs
        KBucketChangeSet kBucketChangeSet = bucket.stale(node);
        BitString kBucketPrefix = bucket.getPrefix();

                // insert last bucket activity time in to bucket update times... it may be null if bucket has never been accessed, in which
        // case we insert MIN instead
        //
        // note that marking a node as stale may have replaced it in the bucket with another node in the cache. That cache node
        // could have an older time than the stale node, meaning that bucketUpdateTimes may actually be older after the replacement!
        Instant lastBucketActivityTime = bucket.getLatestBucketActivityTime();
        if (lastBucketActivityTime == null) {
            lastBucketActivityTime = Instant.MIN;
        }
        bucketUpdateTimes.remove(kBucketPrefix);
        bucketUpdateTimes.insert(lastBucketActivityTime, kBucketPrefix);

        return new RouteTreeChangeSet(kBucketPrefix, kBucketChangeSet);
    }

    // Disable for now -- not being used
//    public void lock(Node node) {
//        Validate.notNull(node);
//
//        Id id = node.getId();
//        InternalValidate.matchesLength(baseId.getBitLength(), id);
//        InternalValidate.notMatchesBase(baseId, id);
//            
//        root.getBucketFor(node.getId()).lock(node);
//    }
//
//    public void unlock(Node node) {
//        Validate.notNull(node);
//
//        Id id = node.getId();
//        InternalValidate.matchesLength(baseId.getBitLength(), id);
//        InternalValidate.notMatchesBase(baseId, id);
//            
//        root.getBucketFor(node.getId()).unlock(node);
//    }

    /**
     * Get prefixes for k-buckets that haven't been updated
     * (from {@link #touch(Instant, Node) }) since the time specified.
     * @param time last update time threshold (k-buckets with their last update time before this get returned by this method)
     * @return prefixes for stagnant k-buckets
     * @throws NullPointerException if any argument is {@code null}
     */
    public List<BitString> getStagnantBuckets(Instant time) { // is inclusive
        Validate.notNull(time);
        
        List<BitString> prefixes = bucketUpdateTimes.getBefore(time, true);
        return prefixes;
    }
    
    private static final BitString EMPTY = BitString.createFromString("");

    private RouteTreeNode createRoot(
            RouteTreeBranchStrategy branchStrategy,
            RouteTreeBucketStrategy bucketStrategy) {
        Validate.notNull(branchStrategy);
        Validate.notNull(bucketStrategy);


        // Get number of branches/buckets to create for root
        int numOfBuckets = branchStrategy.getBranchCount(EMPTY);
        Validate.isTrue(numOfBuckets >= 0, "Branch cannot be negative, was %d", numOfBuckets);
        Validate.isTrue(numOfBuckets != 0, "Root of tree must contain at least 1 branch, was %d", numOfBuckets);
        Validate.isTrue(Integer.bitCount(numOfBuckets) == 1, "Branch count must be power of 2");
        int suffixBitCount = Integer.bitCount(numOfBuckets - 1); // num of bits   e.g. 8 --> 1000 - 1 = 0111, bitcount(0111) = 3
        Validate.isTrue(suffixBitCount <= baseId.getBitLength(),
                "Attempting to branch too far (in root) %d bits extends past %d bits", suffixBitCount, baseId.getBitLength());

        
        // Create buckets by creating a 0-sized top bucket and splitting it + resizing each split
        KBucket[] newBuckets = new KBucket(baseId, EMPTY, 0, 0).split(suffixBitCount);
        for (int i = 0; i < newBuckets.length; i++) {
            KBucketParameters bucketParams = bucketStrategy.getBucketParameters(newBuckets[i].getPrefix());
            int bucketSize = bucketParams.getBucketSize();
            int cacheSize = bucketParams.getCacheSize();
            newBuckets[i].resizeBucket(bucketSize);
            newBuckets[i].resizeCache(cacheSize);
            
            // insert last bucket activity time in to bucket update times... it may be null if bucket has never been accessed, in which case
            // we insert MIN instead
            Instant lastBucketActivityTime = newBuckets[i].getLatestBucketActivityTime();
            if (lastBucketActivityTime == null) {
                lastBucketActivityTime = Instant.MIN;
            }
            bucketUpdateTimes.insert(lastBucketActivityTime, newBuckets[i].getPrefix());
        }

        // Create root
        return new RouteTreeNode(EMPTY, suffixBitCount, newBuckets);
    }

    private RouteTreeNode growParent(RouteTreeNode parent,
            RouteTreeBranchStrategy branchStrategy,
            RouteTreeBucketStrategy bucketStrategy) {
        Validate.notNull(parent);
        Validate.notNull(branchStrategy);
        Validate.notNull(bucketStrategy);

        
        // Calculate which bucket from parent to split
        int parentNumOfBuckets = parent.getBranchCount();
        Validate.validState(Integer.bitCount(parentNumOfBuckets) == 1); // sanity check numofbuckets is pow of 2
        int parentPrefixBitLen = parent.getPrefix().getBitLength(); // num of bits in parent's prefix
        int parentSuffixBitCount = Integer.bitCount(parentNumOfBuckets - 1); // num of bits in parent's suffix
                                                                             // e.g. 8 --> 1000 - 1 = 0111, bitcount(0111) = 3
        
        if (parentPrefixBitLen + parentSuffixBitCount >= baseId.getBitLength()) { // should never be >, only ==, but just in case
            // The parents prefix length + the number of bits the parent used for buckets > baseId's length. As such, it isn't possible to
            // grow any further, so don't even try.
            return null;
        }
        
        int splitBucketIdx = (int) baseId.getBitString().getBitsAsLong(parentPrefixBitLen, parentSuffixBitCount);
        KBucket splitBucket = parent.getBranch(splitBucketIdx).getItem();
        BitString splitBucketPrefix = splitBucket.getPrefix();
        
        
        // Get number of buckets to create for new level
        int numOfBuckets = branchStrategy.getBranchCount(splitBucketPrefix);
        Validate.isTrue(numOfBuckets >= 2, "Branch count must be atleast 2, was %d", numOfBuckets);
        Validate.isTrue(Integer.bitCount(numOfBuckets) == 1, "Branch count must be power of 2");
        int suffixBitCount = Integer.bitCount(numOfBuckets - 1); // num of bits   e.g. 8 (1000) -- 1000 - 1 = 0111, bitcount(0111) = 3
        Validate.isTrue(splitBucketPrefix.getBitLength() + suffixBitCount <= baseId.getBitLength(),
                "Attempting to branch too far %s with %d bits extends past %d bits", splitBucketPrefix, suffixBitCount,
                baseId.getBitLength());
        
        
        // Split parent bucket at that branch index
        BitString newPrefix = baseId.getBitString().getBits(0, parentPrefixBitLen + suffixBitCount);
        KBucket[] newBuckets = splitBucket.split(suffixBitCount);
        for (int i = 0; i < newBuckets.length; i++) {
            KBucketParameters bucketParams = bucketStrategy.getBucketParameters(newBuckets[i].getPrefix());
            int bucketSize = bucketParams.getBucketSize();
            int cacheSize = bucketParams.getCacheSize();
            newBuckets[i].resizeBucket(bucketSize);
            newBuckets[i].resizeCache(cacheSize);

            Instant lastBucketActivityTime = newBuckets[i].getLatestBucketActivityTime();
            if (lastBucketActivityTime == null) {
                lastBucketActivityTime = Instant.MIN;
            }
            bucketUpdateTimes.insert(lastBucketActivityTime, newBuckets[i].getPrefix());
        }

        // Get rid of parent bucket we just split. It branches down at that point, and any nodes that were contained within will be in the
        // newly created buckets
        bucketUpdateTimes.remove(splitBucketPrefix);

        // Create new level and set as child
        RouteTreeNode newNode = new RouteTreeNode(newPrefix, suffixBitCount, newBuckets);
        
        parent.setBranch(splitBucketIdx, new RouteTreeNodeBranch(newNode));
        
        return newNode;
    }
}
