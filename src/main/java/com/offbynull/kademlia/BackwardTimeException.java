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
import org.apache.commons.lang3.Validate;

/**
 * Thrown to indicate that the time value passed in to is less than the last time value passed (meaning that an attempt was made to move
 * backward in time).
 * <p>
 * Class is immutable.
 * @author Kasra Faghihi
 */
public final class BackwardTimeException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    private final Instant previousTime;
    private final Instant inputTime;

    BackwardTimeException(Instant previousTime, Instant inputTime) {
        super("Time is (" + inputTime + ") is before " + previousTime);
        Validate.notNull(previousTime);
        Validate.notNull(inputTime);
        // what's the point of throwing an exception for going backwards in time if you're going forward in time?
        Validate.isTrue(!inputTime.isBefore(previousTime));
        this.previousTime = previousTime;
        this.inputTime = inputTime;
    }

    /**
     * Get the previous time value.
     * @return previous time value
     */
    public Instant getPreviousTime() {
        return previousTime;
    }

    /**
     * Get the input time value.
     * @return input time value
     */
    public Instant getInputTime() {
        return inputTime;
    }
    
}
