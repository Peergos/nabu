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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.Validate;

final class RouteTreeNode {
    
    private final BitString prefix;
    private final int suffixLen;
    private final List<RouteTreeBranch> branches; // branches can contain KBuckets or RouteTreeLevels that are further down

    RouteTreeNode(BitString prefix, int suffixLen, KBucket[] buckets) {
        Validate.notNull(prefix);
        Validate.notNull(buckets);
        Validate.noNullElements(buckets);
        Validate.isTrue(suffixLen > 0);
        
        this.prefix = prefix;
        this.suffixLen = suffixLen;
        this.branches = new ArrayList<>(buckets.length);
        Arrays.stream(buckets)
                .map(x -> new RouteTreeBucketBranch(x))
                .forEachOrdered(branches::add);
    }

    public RouteTreeBranch getBranch(int idx) {
        Validate.isTrue(idx >= 0);
        Validate.isTrue(idx < branches.size());
        
        return branches.get(idx);
    }

    public void setBranch(int idx, RouteTreeBranch branch) {
        Validate.notNull(branch);
        Validate.isTrue(idx >= 0);
        Validate.isTrue(idx < branches.size());
        
        branches.set(idx, branch);
    }

    public int getBranchCount() {
        return branches.size();
    }
    
    public BitString getPrefix() {
        return prefix;
    }

    // id is the id we're trying to find
    // treeset compares against id
    public void findNodesWithLargestPossiblePrefix(Id id, TreeSet<Activity> output, int max, boolean includeStale) {
        Validate.notNull(id);
        Validate.notNull(output);  // technically shouldn't contain any null elements, but we don't care since we're just adding to this
        Validate.isTrue(max >= 0); // why would anyone want 0? let thru anyways
        Validate.isTrue(id.getBitString().getBits(0, prefix.getBitLength()).equals(prefix)); // ensure prefix matches

        // Recursively go down the until you find the branch with the largest matching prefix to ID. Once you find it, call
        // dumpAllNodesUnderTreeNode, and as you pop back up call dumpAllNodesUnderTreeNode again (making sure to not recurse back in to
        // the branch you're coming out of).

        int traverseIdx = (int) id.getBitsAsLong(prefix.getBitLength(), suffixLen);
        RouteTreeBranch traverseBranch = branches.get(traverseIdx);
        BitString traversePrefix = traverseBranch.getPrefix(); //id.getBitString().getBits(0, prefix.getBitLength() + suffixLen);

        if (traverseBranch instanceof RouteTreeNodeBranch) {
            RouteTreeNode treeNode = traverseBranch.getItem();
            treeNode.findNodesWithLargestPossiblePrefix(id, output, max, includeStale);

            dumpAllNodesUnderTreeNode(id, output, max, includeStale, singleton(traversePrefix));
        } else if (traverseBranch instanceof RouteTreeBucketBranch) {
            dumpAllNodesUnderTreeNode(id, output, max, includeStale, emptySet());
        } else {
            throw new IllegalStateException(); // should never happen
        }
    }

    // id is the id we're trying to find
    // treeset compares against id
    public void dumpAllNodesUnderTreeNode(Id id, TreeSet<Activity> output, int max, boolean includeStale, Set<BitString> skipPrefixes) {
        Validate.notNull(id);
        Validate.notNull(output);  // technically shouldn't contain any null elements, but we don't care since we're just adding to this
        Validate.notNull(skipPrefixes);
        Validate.noNullElements(skipPrefixes);
        Validate.isTrue(max >= 0); // why would anyone want 0 here? let thru anwyways

        // No more room in bucket? just leave right away.
        if (output.size() >= max) {
            return;
        }

        // Sort branches at this treenode by how close the are to the ID we're searching for... Go through the sorted branches in
        // order...
        //
        //   If it's a bucket: dump it.
        //   If it's a branch: recurse in to the branch and repeat
        //
        ArrayList<RouteTreeBranch> sortedBranches = new ArrayList<>(branches);
        Collections.sort(sortedBranches, new PrefixClosenessComparator(id, prefix.getBitLength(), suffixLen));

        // What is the point of taking in an ID and sorting the branches in this tree node such that the we access the "closer" prefixes
        // first? We want to access the branches that are closer to the suffix of the ID first because ...
        //
        //
        // 1. Given the same prefix, we don't end up accessing the exact same set of nodes given. For example...
        //
        //      0/\1
        //      /  EMPTY
        //    0/\1
        //    /  FULL
        //  0/\1
        // ME  FULL
        //
        // Assume the routing tree above. We want to route to node 111, but bucket 1xx is empty. We then go down the other branch and
        // start grabbing nodes starting with prefix 0xx. We then use the suffix of 111 (x11) to determine which branches to traverse
        // down first for our 0xx nodes to return. We do this because we don't want to return the same set of nodes everytime someone
        // tries to access a 1xx node and we have an empty branch.
        //
        // For example...
        // if someone wanted 111 and 1xx was empty, path to search under 0xx would be 011, then 001, then 000.
        // if someone wanted 101 and 1xx was empty, path to search under 0xx would be 001, then 000, then 011.
        //
        // If we did something like a depth-first search, we'd always target 000 first, then 001, then 011. We don't want to do that
        // because we don't want to return the same set of nodes everytime. It would end up being an undue burden on those nodes.
        //
        //
        //
        // 2. Remember our notion of closeness: XOR and normal integer less-than to see which is closer. So for example, lets say we're
        // looking for ID 111110 and the prefix at this point in the tree is is 110xxx. Even though the prefix 110 doesn't match, we
        // still want to match as closely to the remaining suffix as possible, because when we XOR those extra 0's at the beginning of 
        // the suffix mean that we're closer.
        //
        // For example...
        //
        // This tree node has the prefix 110xxx and the ID we're searching for is 111110. There are 2 branches at this tree node:
        // 1100xx and 1101xx
        //
        //      110xxx
        //        /\
        //       /  \
        //      /    \
        //   0 /      \ 1
        //    /        \
        // 1100xx    1101xx
        //
        // We know that for ID 111110, the IDs under 1101xx WILL ALWAYS BE CLOSER than the IDs at 1100xx.
        //
        // XORing with the 1100xx bucket ... XOR(111110, 1100xx) = 0011xx
        // 
        // XORing with the 1101xx bucket ... XOR(111110, 1101xx) = 0010xx
        //
        //
        //     Remember how < works... go compare each single bit from the beginning until you come across a pair of bits that aren't
        //     equal (one is 0 and the other is 1). The ID with 0 at that position is less-than the other one.
        //
        //
        // The one on the bottom (1101xx) will ALWAYS CONTAIN CLOSER IDs...
        //
        // An example ID in top:    110011 ... XOR(111110, 110011) = 001101 = 13
        // An exmaple ID in bottom: 110100 ... XOR(111110, 110100) = 001010 = 9
        // 

        for (RouteTreeBranch sortedBranch : sortedBranches) {
            if (skipPrefixes.contains(sortedBranch.getPrefix())) {
                continue;
            }

            if (sortedBranch instanceof RouteTreeNodeBranch) {
                RouteTreeNode node = sortedBranch.getItem();
                node.dumpAllNodesUnderTreeNode(id, output, max, includeStale, emptySet()); // dont propogate skipPrefixes (not relevant)

                // Bucket's full after dumping nodes in that branch. No point in continued processing.
                if (output.size() >= max) {
                    return;
                }
            } else if (sortedBranch instanceof RouteTreeBucketBranch) {
                KBucket bucket = sortedBranch.getItem();

                // don't bother with locked nodes for now, we're not supporting them
                output.addAll(bucket.dumpBucket(true, includeStale, false));

                // Bucket's full after that add. No point in continued processing.
                if (output.size() >= max) {
                    // If we have more than max elements from that last add, start evicting farthest away nodes
                    while (output.size() > max) {
                        output.pollLast();
                    }
                    return;
                }
            } else {
                throw new IllegalStateException(); // should never happen
            }
        }
    }

    public KBucket getBucketForPrefix(BitString searchPrefix) {
        Validate.notNull(searchPrefix);
        Validate.isTrue(searchPrefix.getBits(0, prefix.getBitLength()).equals(prefix)); // ensure prefix of searchPrefix matches

        int bucketIdx = (int) searchPrefix.getBitsAsLong(prefix.getBitLength(), suffixLen);
        RouteTreeBranch branch = branches.get(bucketIdx);

        if (branch instanceof RouteTreeNodeBranch) {
            RouteTreeNode node = branch.getItem();
            return node.getBucketForPrefix(searchPrefix);
        } else if (branch instanceof RouteTreeBucketBranch) {
            KBucket bucket = branch.getItem();
            return bucket;
        } else {
            throw new IllegalStateException(); // should never happen
        }
    }

    public KBucket getBucketFor(Id id) {
        Validate.notNull(id);
        Validate.isTrue(id.getBitString().getBits(0, prefix.getBitLength()).equals(prefix)); // ensure prefix matches

        int bucketIdx = (int) id.getBitsAsLong(prefix.getBitLength(), suffixLen);
        RouteTreeBranch branch = branches.get(bucketIdx);

        if (branch instanceof RouteTreeNodeBranch) {
            RouteTreeNode treeNode = branch.getItem();
            return treeNode.getBucketFor(id);
        } else if (branch instanceof RouteTreeBucketBranch) {
            KBucket bucket = branch.getItem();
            return bucket;
        } else {
            throw new IllegalStateException(); // should never happen
        }
    }    
    
    public void dumpAllBucketPrefixes(List<BitString> output) {
        Validate.notNull(output); // technically shouldn't contain any null elements, but we don't care since we're just adding to this
        
        for (RouteTreeBranch branch : branches) {
            if (branch instanceof RouteTreeNodeBranch) {
                RouteTreeNode treeNode = branch.getItem();
                treeNode.dumpAllBucketPrefixes(output);
            } else if (branch instanceof RouteTreeBucketBranch) {
                output.add(branch.getPrefix());
            } else {
                throw new IllegalStateException(); // should never happen
            }
        }
    }    
        
        
    private static final class PrefixClosenessComparator implements Comparator<RouteTreeBranch>, Serializable {
        private static final long serialVersionUID = 1L;
        
        // This is a hacky way to compare bitstrings using the XOR metric intended for IDs
        private final int prefixLen;
        private final int suffixLen;
        private final IdXorMetricComparator partialIdClosenessComparator;

        PrefixClosenessComparator(Id id, int prefixLen, int suffixLen) {
            Validate.notNull(id);
            Validate.isTrue(prefixLen >= 0);
            Validate.isTrue(suffixLen > 0);
            Validate.isTrue(id.getBitLength() >= prefixLen + suffixLen);
            
            this.prefixLen = prefixLen;
            this.suffixLen = suffixLen;
            
            Id partialId = Id.create(id.getBitString().getBits(prefixLen, suffixLen));
            this.partialIdClosenessComparator = new IdXorMetricComparator(partialId);
        }

        @Override
        public int compare(RouteTreeBranch o1, RouteTreeBranch o2) {
            Validate.isTrue(o1.getPrefix().getBitLength() == prefixLen + suffixLen);
            Validate.isTrue(o2.getPrefix().getBitLength() == prefixLen + suffixLen);
            
            return partialIdClosenessComparator.compare(
                    Id.create(o1.getPrefix().getBits(prefixLen, suffixLen)),
                    Id.create(o2.getPrefix().getBits(prefixLen, suffixLen)));
        }
    }
}
