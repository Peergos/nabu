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

import org.apache.commons.lang3.Validate;

/**
 * Thrown to indicate that the link of input node didn't match with the link of an existing node with the same ID.
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class LinkMismatchException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    private final Node conflictingNode;
    private final String expectedLink;

    LinkMismatchException(Node conflictingNode, String expectedLink) {
        super("Node link mismatch (required " + expectedLink +  "): " + conflictingNode + ")");
        Validate.notNull(conflictingNode);
        Validate.notNull(expectedLink);
        this.conflictingNode = conflictingNode;
        this.expectedLink = expectedLink;
    }

    /**
     * Get the input node.
     * @return input node
     */
    public Node getConflictingNode() {
        return conflictingNode;
    }

    /**
     * Get the link that should have been in the input node.
     * @return expected link
     */
    public String getExpectedLink() {
        return expectedLink;
    }

}
