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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * An implementation of Kademlia's k-bucket. This implementation aligns to the requirements given in the original Kademlia paper, in that
 * it ...
 * <ul>
 * <li>nodes stored have the same pre-defined prefix</li>
 * <li>there's a replacement cache of nodes (most recently seen)</li>
 * <li>allows marking a node as stale -- which will cause it to be replaced if a node becomes available in the replacement cache</li>
 * <li>allows marking a node as locked -- which will temporarily ignore it</li>
 * <li>allows splitting of a k-bucket</li>
 * </ul>
 * @author Kasra Faghihi
 */
public final class KBucket {

    private final Id baseId;

    private final BitString prefix;

    // the way thigns are done, cache and bucket should never contain the same node id at the same time
    private final NodeLeastRecentSet bucket;
    private final NodeMostRecentSet cache;
    
    // i thought about using predicates instead of internally holding on to this information in the sets below but accepting predicates from
    // the outside introduces problems: 1. design becomes more convoluted / less understandable 2. the logic for which an item is determined
    // to be stale or locked may change without this kbucket ever knowing... which means that everytime touch is called we need to go
    // through the buckets, query the predicates for each node, and move around nodes...
    //
    // the way things are done, these 2 sets should only ever contain nodes from bucket
    private final Set<Id> staleSet; // stale nodes are returned by dumpBucket?
    private final Set<Id> lockSet; // locked nodes aren't returned by dumpBucket?

    private Instant lastTouchAttemptTime;

    /**
     * Constructs a {@link KBucket} object.
     * @param baseId ID of the node this k-bucket belongs to
     * @param prefix prefix that nodes stored in this k-bucket must have
     * @param maxBucketSize maximum number of nodes allowed in this k-bucket (the k value)
     * @param maxCacheSize maximum number of nodes allowed in this k-bucket's replacement cache
     * @throws NullPointerException if any argument is {@code null}
     * @throws IllegalArgumentException if {@code prefix.getBitLength() > baseId.getBitLength()}, or if any numeric argument is less than
     * {@code 0}
     */
    public KBucket(Id baseId, BitString prefix, int maxBucketSize, int maxCacheSize) {
        Validate.notNull(baseId);
        Validate.isTrue(prefix.getBitLength() <= baseId.getBitLength());
        // Let this thru anyways, because without it bucket splitting logic will become slightly more convolouted. That is, in a certain
        // case a bucket would be split such that one of the new buckets may == baseId.
//        Validate.isTrue(!baseId.getBitString().equals(prefix)); // baseId cannot == prefix, because then you'd have an empty bucket that
//                                                                // you can't add anything to ... no point in having a bucket with your
//                                                                // own ID in it
        Validate.isTrue(maxBucketSize >= 0); // what's the point of a 0 size kbucket? let it thru anyways
        Validate.isTrue(maxCacheSize >= 0); // a cache size of 0 is not worthless...  may not care about having a replacement cache of nodes

        this.baseId = baseId;
        this.prefix = prefix;
        this.bucket = new NodeLeastRecentSet(baseId, maxBucketSize);
        this.cache = new NodeMostRecentSet(baseId, maxCacheSize);
        this.staleSet = new LinkedHashSet<>(); // maintain order they're added, when replacing we want to replace oldest stale first
        this.lockSet = new HashSet<>();
        
        lastTouchAttemptTime = Instant.MIN;
    }

    /**
     * Updates the k-bucket with a new contact (potentially). When the owning Kademlia node receives a request or response from some other
     * node in the network which has an ID matching the prefix of this k-bucket, this method should be called.
     * <p>
     * When this method this called, it attempts to store the node. 
     * <ul>
     * <li>If this k-bucket is full, it places the node in to its replacement cache (evicting the oldest node in the replacement cache if
     * the replacement cache is full)</li>
     * <li>If this k-bucket is full but there are stale nodes in the bucket and the replacement cache is empty, one of the stale nodes is
     * evicted and the new node is added in to the bucket.</li>
     * <li>If the contacting node already exists but is stale, revert to that node to normal status (unmark it as stale).</li>
     * </ul>
     * @param time time which request or response came in
     * @param node node which issued the request or response
     * @return changes to collection of stored nodes and replacement cache
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this k-bucket belongs to)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this k-bucket belongs to)
     * @throws IdPrefixMismatchException if {@code node}'s ID doesn't match the prefix required by this k-bucket
     * @throws BackwardTimeException if {@code time} is less than the time used in the previous invocation of this method
     * @throws LinkMismatchException if this k-bucket already contains a node with {@code node}'s ID but with a different link (SPECIAL
     * CASE: If the contained node is marked as stale, this exception will not be thrown. Since the node is marked as stale, it means it
     * should have been replaced but the replacement cache was empty. As such, this case is treated as if this were a new node replacing
     * a stale node, not a stale node being reverted to normal status -- the fact that the IDs are the same but the links don't match
     * doesn't matter)
     */
    public KBucketChangeSet touch(Instant time, Node node) {
        Validate.notNull(time);
        Validate.notNull(node);

        Id nodeId = node.getId();

        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
        InternalValidate.notMatchesBase(baseId, nodeId);
        InternalValidate.matchesPrefix(prefix, nodeId);

        InternalValidate.forwardTime(lastTouchAttemptTime, time); // time must be >= lastUpdatedTime
        lastTouchAttemptTime = time;
        
        // Touch the bucket
        //
        // SPECIAL CASE: If the touch is from a ID that's in the stale set but has a different link, let it through (DO NOT THROW A
        // LINKMISMATCHEXCEPTION). Since the ID is marked as being stale, it means it needs to be replaced but there were no other items in
        // the cache to replace it with. As such, just treat it as if we're replacing an item with a new cache item.
        ActivityChangeSet bucketTouchRes = bucket.touch(time, node, staleSet.contains(nodeId));
        Validate.validState(bucketTouchRes.viewRemoved().isEmpty()); // sanity check, should never remove anything when touching bucket
        if (!bucketTouchRes.viewAdded().isEmpty() || !bucketTouchRes.viewUpdated().isEmpty()) {
            // node was added to bucket, or node was already in bucket and was updated
            staleSet.remove(nodeId); // if being updated, node may have been stale... unstale it here because it's being touched
            // DO NOT UNLOCK ON TOUCH, when need to explicitly unlock elsewhere
            return new KBucketChangeSet(bucketTouchRes, ActivityChangeSet.NO_CHANGE);
        }
        
        
        
        // Touch the cache
        //
        // Do not throw a LinkMismatchException if the node touching the cache is already in the cache (ID is the same) but has a different
        // link, because these are just cache nodes. We don't need to maintain their integrity until they get promoted to the bucket.
        ActivityChangeSet cacheTouchRes = cache.touch(time, node, true);
        
        // There may be something in the cache now, so if we have any stale nodes, replace them with this new cache item. We should never
        // ever be in a state where !cache.isEmpty() && !staleSet.isEmpty(). If we are then something's gone wrong.
        // left = removed stale node from bucket
        // right = moved in to bucket the node that was jsut added in to cache
        ImmutablePair<Activity, Activity> res = replaceNextStaleNodeWithCacheNode(); // left = removed, right = added
        if (res != null) {
            return new KBucketChangeSet(
                    new ActivityChangeSet(singletonList(res.right), singletonList(res.left), emptyList()),
                    ActivityChangeSet.NO_CHANGE); // nochange because technically nothing moved in to cache, even though it temporarily did
        }
        
        
        
        // No stale nodes encountered, so nothing was replaced. Return standard results.
        return new KBucketChangeSet(bucketTouchRes, cacheTouchRes);
    }
    
    /**
     * Marks a node within this k-bucket as stale (meaning that you're no longer able to communicate with it), evicting it and replacing it
     * with the most recent node in the replacement cache. If the replacement cache is empty, the node is marked as stale and will be
     * replaced once a node becomes available in the replacement cache.
     * <p>
     * If the node is marked as stale, but is touched ({@link #touch(Instant, Node) } before it
     * could be evicted, it reverts back to normal state (is unmarked as stale -- effectively meaning it came back online).
     * <p>
     * If the node has already stale, this method does nothing.
     * @param node node to mark as stale
     * @return changes to collection of stored nodes and replacement cache (this method should have no effect on the replacement cache)
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this k-bucket belongs to)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this k-bucket belongs to)
     * @throws IdPrefixMismatchException if {@code node}'s ID doesn't match the prefix required by this k-bucket
     * @throws NodeNotFoundException if this k-bucket doesn't contain {@code node}
     * @throws LinkMismatchException if this k-bucket contains a node with {@code node}'s ID but with a different link
     * @throws BadNodeStateException if this k-bucket contains {@code node} but {@code node} is marked as locked
     */
    public KBucketChangeSet stale(Node node) {
        // there's no time param here because technically because it isn't needed. marking a node as stale doesn't mean that it recieved
        // comm, as such it's wrong to update its time.
        Validate.notNull(node);
        
        Id nodeId = node.getId();

        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
        InternalValidate.notMatchesBase(baseId, nodeId);
        InternalValidate.matchesPrefix(prefix, nodeId);
        
        InternalValidate.exists(node, bucket); // node being marked as stale must be in bucket
        InternalValidate.correctState(node, !lockSet.contains(nodeId)); // node locked, cannot enter stale state (stale / locked are mutex)

        staleSet.add(nodeId); // add to stale set, it's fine if it's already in the staleset
        
        // replace, if nodes are available in cache to replace with... otherwise it'll just keep this node marked as stale
        // left = removed stale node from bucket
        // right = moved in to bucket from cache in order to replace stale node
        ImmutablePair<Activity, Activity> res = replaceNextStaleNodeWithCacheNode();
        if (res == null) {
            // There were no nodes in cache to move over, as such return no change. But as soon as a cache node becomes available it'll be
            // used as a replacement for nodes in the stale set (see touch())
            return new KBucketChangeSet(ActivityChangeSet.NO_CHANGE, ActivityChangeSet.NO_CHANGE);
        }

        
        return new KBucketChangeSet(
                new ActivityChangeSet(singletonList(res.right), singletonList(res.left), emptyList()),
                ActivityChangeSet.removed(res.right));
    }
    
    /**
     * Marks a node within this k-bucket as locked (meaning that you're temporarily ignoring, possibly because of network congestion).
     * <p>
     * If the node is marked as locked, touching it ({@link #touch(Instant, Node) } has
     * no effect on it. It reverts back to normal state (unlocks) when {@link #unlock(Node) } is invoked.
     * <p>
     * If the node has already locked, this method does nothing.
     * @param node node to mark as locked
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this k-bucket belongs to)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this k-bucket belongs to)
     * @throws IdPrefixMismatchException if {@code node}'s ID doesn't match the prefix required by this k-bucket
     * @throws NodeNotFoundException if this k-bucket doesn't contain {@code node}
     * @throws LinkMismatchException if this k-bucket contains a node with {@code node}'s ID but with a different link
     * @throws BadNodeStateException if this k-bucket contains {@code node} but {@code node} is marked as stale
     */
    public void lock(Node node) {
        Validate.notNull(node);
        
        Id nodeId = node.getId();

        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
        InternalValidate.notMatchesBase(baseId, nodeId);
        InternalValidate.matchesPrefix(prefix, nodeId);
        
        InternalValidate.exists(node, bucket);  // node being marked as locked must be in bucket
        InternalValidate.correctState(node, !staleSet.contains(nodeId)); // node stale, cannot enter locked state (stale / locked are mutex)

        lockSet.add(nodeId); // add to lock set, it's fine if it's already in the lockset
    }

    /**
     * Reverts a node back to a normal state from a locked state.
     * <p>
     * If the node is not locked, this method does nothing.
     * @param node node to revert to normal state from locked state
     * @throws NullPointerException if any argument is {@code null}
     * @throws IdLengthMismatchException if the bitlength of {@code node}'s ID doesn't match the bitlength of the owning node's ID (the ID
     * of the node this k-bucket belongs to)
     * @throws BaseIdMatchException if {@code node}'s ID is the same as the owning node's ID (the ID of the node this k-bucket belongs to)
     * @throws IdPrefixMismatchException if {@code node}'s ID doesn't match the prefix required by this k-bucket
     * @throws NodeNotFoundException if this k-bucket doesn't contain {@code node}
     * @throws LinkMismatchException if this k-bucket contains a node with {@code node}'s ID but with a different link
     * @throws BadNodeStateException if this k-bucket contains {@code node} but {@code node} is marked as stale
     */
    public void unlock(Node node) {
        Validate.notNull(node);
        
        Id nodeId = node.getId();

        InternalValidate.matchesLength(baseId.getBitLength(), nodeId);
        InternalValidate.notMatchesBase(baseId, nodeId);
        InternalValidate.matchesPrefix(prefix, nodeId);
        
        InternalValidate.exists(node, bucket);  // node being marked as locked must be in bucket
        InternalValidate.correctState(node, !staleSet.contains(nodeId)); // node stale, cannot be in locked state (stale / locked are mutex)

        lockSet.remove(nodeId); // remove from lock set, it's fine if it's already in the lockset
    }

    // return is left=removed right=added
    private ImmutablePair<Activity, Activity> replaceNextStaleNodeWithCacheNode() {
        if (staleSet.isEmpty()) {
            return null;
        }
        
        // Get stale node
        Iterator<Id> staleIt = staleSet.iterator();
        Id staleId = staleIt.next();
        
        // Check to make sure cache has items to replace with
        if (cache.size() == 0) {
            return null;
        }
        
        // Remove from bucket and staleset
        staleIt.remove(); // remove from staleset
        Node staleNode = bucket.get(staleId);
        ActivityChangeSet bucketRemoveRes = bucket.remove(staleNode); // throws EntryConflictException if id is equal but link isn't
        if (bucketRemoveRes.viewRemoved().isEmpty()) {
            return null;
        }
        
        // Remove latest from cache and add to bucket
        ActivityChangeSet cacheRemoveRes = cache.removeMostRecent(1);
        ActivityChangeSet bucketTouchRes;
        Validate.validState(cacheRemoveRes.viewRemoved().size() == 1); // sanity check, should always remove 1 node
        Activity cacheEntry = cacheRemoveRes.viewRemoved().get(0);
        try {
            bucketTouchRes = bucket.touch(cacheEntry.getTime(), cacheEntry.getNode(), false);
        } catch (LinkMismatchException ece) {
            // should never throw EntryConflictException
            throw new IllegalStateException(ece);
        }
        Validate.validState(bucketTouchRes.viewAdded().size() == 1); // sanity check, should always add 1 node
        
        return ImmutablePair.of(bucketRemoveRes.viewRemoved().get(0), bucketTouchRes.viewAdded().get(0));
    }
    
    /**
     * Splits this k-bucket. This method will generate k-buckets with {@code bitCount} extra bits in the prefix, and each generated k-bucket
     * will contain the appropriate nodes (and cache nodes) from this k-bucket.
     * <p>
     * For example, if this bucket's prefix is {@code 1010} and {@code bitCount = 2}, the returning array would contain the following
     * k-buckets (in order) ...
     * <ol>
     * <li>prefix is {@code 1010 00}   note that 0 = 00</li>
     * <li>prefix is {@code 1010 01}   note that 1 = 01</li>
     * <li>prefix is {@code 1010 10}   note that 2 = 10</li>
     * <li>prefix is {@code 1010 11}   note that 3 = 11</li>
     * </ol>
     * Note that {@code bitCount = 2}, which means each generated k-bucket should have 2 extra bits in its prefix. {@code 2^2 = 4}, which
     * results in total of 4 buckets being generated to span those extra 2 bits.
     * @param bitCount number of extra bits in the prefix of the generated k-buckets -- k-buckets generated will be this {@code 2^bitCount}
     * (e.g. bitCount = 0 is 1 buckets, bitCount = 1 is 2 buckets, bitCount = 2 is 4 buckets, bitCount = 3 is 8 buckets, ...)
     * @return k-buckets generated from the split, in order
     * @throws IllegalArgumentException if {@code bitCount < 0} or if {@code bitCount > 30} or if
     * {@code prefix.getBitLength() + bitCount > baseId.getBitLength()}
     */
    public KBucket[] split(int bitCount) {
        Validate.isTrue(bitCount >= 0); // why would anyone want to split in to 1 bucket? the result would just be a copy of this bucket...
                                        // let through anyway
        Validate.isTrue(bitCount <= 30); // its absurd to check for this, as no one will ever want to split in to 2^30 buckets, but whatever
                                         // we can't have more than 30 bits, because 31 bits will result in an array of neg size...
                                         // new Bucket[1 << 31] -- 1 << 31 is negative
                                         // new Bucket[1 << 30] -- 1 << 30 is positive
        
        Validate.isTrue(prefix.getBitLength() + bitCount <= baseId.getBitLength());

        // Create new buckets ...
        // Generates an array of buckets of 2^bitCount elements, where each bucket i has the current bucket's prefix with i appended to it.
        //
        // So for example, if this bucket's prefix = 1010 and bitCount = 2, the returning array would contain buckets with prefixes ...
        //
        // ret[0] = 1010 00   note that 0 = 00
        // ret[1] = 1010 01   note that 1 = 01
        // ret[2] = 1010 10   note that 2 = 10
        // ret[3] = 1010 11   note that 3 = 11
        //
        // note that bitCount = 2, 2^2 = 4, which results in 4 elements
        int maxBucketSize = bucket.maxSize();
        int maxCacheSize = cache.maxSize();
        int len = 1 << bitCount;
        KBucket[] newKBuckets = new KBucket[len];
        for (int i = 0; i < len; i++) {
            BitString numAsBitString = toBitString(i, bitCount);
            BitString appendedBitString = prefix.appendBits(numAsBitString);
            newKBuckets[i] = new KBucket(baseId, appendedBitString, maxBucketSize, maxCacheSize);
            newKBuckets[i].lastTouchAttemptTime = lastTouchAttemptTime; // keep touch attempt time updated
        }
        
        
        // Move from original bucket to new buckets
        for (Activity entry : bucket.dump()) {
            Node node = entry.getNode();
            
            // Read bitCount bits starting from prefixBitSize and use that to figure out which bucket to copy to
            // For example, if bitCount is 2 ...
            // If you read 00b, 00 = 0, so this ID will be go to newBucket[0]
            // If you read 01b, 01 = 1, so this ID will be go to newBucket[1]
            // If you read 10b, 10 = 2, so this ID will be go to newBucket[2]
            // If you read 11b, 11 = 3, so this ID will be go to newBucket[3]
            Id id = node.getId();
            int idx = (int) id.getBitsAsLong(prefix.getBitLength(), bitCount);
            
            // Touch bucket and mark as stale
            ActivityChangeSet res;
            try {
                res = newKBuckets[idx].bucket.touch(entry.getTime(), node, false);
                // FYI: If there are stale items, it means the cache is empty. Otherwise they would have been replaced if as soon as a cache
                // node entered the bucket.
                if (staleSet.contains(id)) {
                    newKBuckets[idx].staleSet.add(id);
                }
                // move over lock nodes as well
                if (lockSet.contains(id)) {
                    newKBuckets[idx].lockSet.add(id);
                }
            } catch (LinkMismatchException ece) {
                // should never happen
                throw new IllegalStateException(ece);
            }
            Validate.validState(!res.viewAdded().isEmpty()); // sanity check, should always add
        }

        
        // Move from original cache to new cache
        for (Activity entry : cache.dump()) {
            Node node = entry.getNode();
            
            // Read bitCount bits starting from prefixBitSize and use that to figure out which bucket to copy to
            // For example, if bitCount is 2 ...
            // If you read 00b, 00 = 0, so this ID will be go to newBucket[0]
            // If you read 01b, 01 = 1, so this ID will be go to newBucket[1]
            // If you read 10b, 10 = 2, so this ID will be go to newBucket[2]
            // If you read 11b, 11 = 3, so this ID will be go to newBucket[3]
            Id id = node.getId();
            int idx = (int) id.getBitsAsLong(prefix.getBitLength(), bitCount);
            
            // Touch cache
            ActivityChangeSet res;
            try {
                res = newKBuckets[idx].cache.touch(entry.getTime(), node, false);
            } catch (LinkMismatchException ece) {
                // should never happen
                throw new IllegalStateException(ece);
            }
            Validate.validState(!res.viewAdded().isEmpty()); // sanity check, should always add
        }
        
        
        // Now that this is has been split in to multiple kbuckets, each kbucket's bucket may not be as full as possible. Try to move over
        // nodes from the cache to the bucket
        for (int i = 0; i < len; i++) {
            newKBuckets[i].fillMissingBucketSlotsWithCacheItems();
        }
        
        
        return newKBuckets;
    }
    
    /**
     * Resizes the k-bucket.
     * <p>
     * If the new size is less than the old size, nodes need to be evicted. The nodes that haven't been touched the longest are removed
     * first.
     * <p>
     * If the new size is greater than the old size, nodes are moved from the replacement cache in to the newly empty slots. The most
     * recently touched nodes in the replacement cache are moved first.
     * @param maxSize new size
     * @return changes as a result of the resize
     */
    public KBucketChangeSet resizeBucket(int maxSize) {
        Validate.isTrue(maxSize >= 0);
        
        if (maxSize <= bucket.maxSize()) {
            // reducing space
            ActivityChangeSet res = bucket.resize(maxSize);
            
            // sanity check
            // validate nothing was added or updated -- the only thing that can happen is elements can be removed
            Validate.validState(res.viewAdded().isEmpty());
            Validate.validState(res.viewUpdated().isEmpty());
            
            // all nodes that were removed from bucket need to also be removed in staleness set
            res.viewRemoved().forEach(x -> staleSet.remove(x.getNode().getId()));
            
            return new KBucketChangeSet(res, ActivityChangeSet.NO_CHANGE);
        } else {
            // increasing space, so move over stuff from the cache in to new bucket spaces
            ActivityChangeSet res = bucket.resize(maxSize);
            
            // sanity check
            // validate nothing changed with elements in the set -- we're only expanding the size of the bucket
            Validate.validState(res.viewAdded().isEmpty());
            Validate.validState(res.viewRemoved().isEmpty());
            Validate.validState(res.viewUpdated().isEmpty());
            
            
            return fillMissingBucketSlotsWithCacheItems();
        }
    }

    /**
     * Resizes the replacement cache. If the new size is less than the old size, nodes need to be evicted from the replacement cache. The
     * nodes in the replacement cache that haven't been touched the longest are removed first.
     * @param maxSize new replacement cache size
     * @return changes as a result of the resize (the only changes that can happen is nodes being removed from the cache)
     */
    public KBucketChangeSet resizeCache(int maxSize) {
        Validate.isTrue(maxSize >= 0);
        
        ActivityChangeSet res = cache.resize(maxSize);
        return new KBucketChangeSet(ActivityChangeSet.NO_CHANGE, res);
    }

    /**
     * Dump nodes in this k-buckets (not replacement cache).
     * @param includeAlive if {@code true}, returned nodes include nodes in normal state
     * @param includeStale if {@code true}, returned nodes include stale nodes
     * @param includeLocked if {@code true}, returned nodes include locked nodes
     * @return nodes in k-bucket
     */
    public List<Activity> dumpBucket(boolean includeAlive, boolean includeStale, boolean includeLocked) {
        List<Activity> dumpedNodes = bucket.dump();
        
        List<Activity> filteredDumpedNodes = new ArrayList<>(dumpedNodes.size());
        dumpedNodes.stream()
                .filter(x -> {
                    boolean inStaleSet = staleSet.contains(x.getNode().getId());
                    if (includeStale && inStaleSet) {
                        return true;
                    }

                    boolean inLockSet = lockSet.contains(x.getNode().getId());
                    if (includeLocked && inLockSet) {
                        return true;
                    }

                    if (includeAlive && !inLockSet && !inStaleSet) {
                        return true;
                    }

                    return false;
                })
                .forEachOrdered(filteredDumpedNodes::add);
        
        return filteredDumpedNodes;
    }

    /**
     * Dump nodes in this k-buckets replacement cache.
     * @return nodes in replacement cache
     */
    public List<Activity> dumpCache() {
        return cache.dump();
    }

    /**
     * Get the time a node in this k-bucket was last touched (not replacement cache).
     * @return last node touch time
     */
    public Instant getLatestBucketActivityTime() {
        return bucket.lastestActivityTime();
    }

    /**
     * Get the time the replacement cache was last touched.
     * @return last cache touch time
     */
    public Instant getLatestCacheActivityTime() {
        return cache.lastestActivityTime();
    }

    /**
     * Get the prefix required for IDs in this k-bucket.
     * @return prefix required for IDs
     */
    public BitString getPrefix() {
        return prefix;
    }
    
    private KBucketChangeSet fillMissingBucketSlotsWithCacheItems() {
        int unoccupiedBucketSlots = bucket.maxSize() - bucket.size();
        int availableCacheItems = cache.size();
        if (unoccupiedBucketSlots <= 0 || availableCacheItems == 0) {
            return new KBucketChangeSet(ActivityChangeSet.NO_CHANGE, ActivityChangeSet.NO_CHANGE);
        }
        
        int moveAmount = Math.min(availableCacheItems, unoccupiedBucketSlots);
        
        ActivityChangeSet cacheRemoveRes = cache.removeMostRecent(moveAmount);
        Validate.validState(cacheRemoveRes.viewAdded().isEmpty());
        Validate.validState(cacheRemoveRes.viewRemoved().size() == moveAmount); // sanity check
        Validate.validState(cacheRemoveRes.viewUpdated().isEmpty());

        for (Activity entryToMove : cacheRemoveRes.viewRemoved()) {
            // move
            ActivityChangeSet addRes;
            try {
                addRes = bucket.touch(entryToMove.getTime(), entryToMove.getNode(), false);
            } catch (LinkMismatchException ece) {
                // This should never happen. The way the logic in this class is written, you should never have an entry with the same id in
                // the cache and the bucket at the same time. As such, it's impossible to encounter a conflict.
                throw new IllegalStateException(ece); // sanity check
            }
            
            // sanity check
            Validate.validState(addRes.viewAdded().size() == 1);
            Validate.validState(addRes.viewRemoved().isEmpty());
            Validate.validState(addRes.viewUpdated().isEmpty());
        }
        
        // show moved as being added to bucket and removed from cache
        return new KBucketChangeSet(ActivityChangeSet.added(cacheRemoveRes.viewRemoved()), cacheRemoveRes);
    }
    
    // The int {@code 0xABCD} with a bitlength of 12 would result in the bit string {@code 10 1011 1100 1101}.
    // Bit     15 14 13 12   11 10 09 08   07 06 05 04   03 02 01 00
    //         ------------------------------------------------------
    //         1  0  1  0    1  0  1  1    1  1  0  0    1  1  0  1
    //         A             B             C             D
    //               ^                                            ^
    //               |                                            | 
    //             start                                         end
    private static BitString toBitString(int data, int bitLength) {
        Validate.notNull(data);
        Validate.isTrue(bitLength >= 0);

        data = data << (32 - bitLength);
        return BitString.createReadOrder(toBytes(data), 0, bitLength);
    }
    
    private static byte[] toBytes(int data) { // returns in big endian format
        byte[] bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            int shiftAmount = 24 - (i * 4);
            bytes[i] = (byte) ((data >>> shiftAmount) & 0xFF);
        }
        return bytes;
    }

    @Override
    public String toString() {
        return "KBucket{" + "baseId=" + baseId + ", prefix=" + prefix + ", bucket=" + bucket + ", cache=" + cache + ", staleSet=" + staleSet
                + ", lastUpdateTime=" + lastTouchAttemptTime + '}';
    }
    
    
}
