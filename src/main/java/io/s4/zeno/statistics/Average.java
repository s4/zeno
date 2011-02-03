/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.zeno.statistics;

/**
 * Running average of a sequence of values.
 */
public abstract class Average {

    private boolean _e = true;

    /**
     * Put a value in the sequence.
     * 
     * @param x
     *            value
     */
    public final void put(double x) {
        update(x);
        _e = false;
    }

    /**
     * Is the sequence empty?
     * 
     * @return true, if empty. False if atleast one value has been put.
     */
    public final boolean empty() {
        return _e;
    }

    /**
     * Update the state with a value. This is where concrete implementations
     * perform computations.
     * 
     * @param x
     *            the value
     */
    protected abstract void update(double x);

    /**
     * Get the current average.
     * 
     * @return the average
     */
    public abstract double get();

    /**
     * Compute running average assuming last element of sequence is {@code y}.
     * Don't actually include it in the sequence.
     * 
     * @param y
     *            hypothetical last value of sequence.
     * @return average assuming last value of sequence is {@code y}
     */
    public abstract double phantomGet(double y);
}
