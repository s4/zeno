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
 * Exponential moving average of a sequence of values.
 */
public class ExponentialMovingAverage extends Average {

    // weighted sum
    private volatile double s = 0.0;

    // total weight so far: asymptotically tends to 1.
    private volatile double w = 0.0;

    // weight for last element of sequence.
    private final double a;

    /**
     * Instantiate with weight.
     * 
     * @param a
     *            weight for last eleent of sequence.
     */
    public ExponentialMovingAverage(double a) {
        this.a = a;
    }

    /**
     * Update moving average for a sequence of values x<sub>1</sub>,
     * x<sub>2</sub>, ..., x<sub>i</sub>
     * 
     * <pre>
     *    s<sub>i</sub> = (1 - a) * s<sub>i-1</sub> + a * x<sub>i</sub>
     *    w<sub>i</sub> = (1 - a) * w<sub>i-1</sub> + a
     *    
     *    s<sub>0</sub> = 0
     *    w<sub>0</sub> = 0
     *    
     *    For i > 0: avg<sub>i</sub> = s<sub>i</sub> / w<sub>i</sub>
     * 
     * @see io.s4.zeno.statistics.Average#update(double)
     */
    protected void update(double x) {
        s = (1 - a) * s + a * x;
        w = (1 - a) * w + a;
    }

    public double get() {
        return s / w;
    }

    /**
     * Moving average for a sequence of values x<sub>1</sub>, x<sub>2</sub>, ...
     * assuming a hypothetical value y as the last element of sequence.
     * 
     * <pre>
     *    s = (1 - a) * s<sub>i</sub> + a * y
     *    w = (1 - a) * w<sub>i</sub> + a
     *    
     *        avg = s / w
     *    
     *    s<sub>i</sub> = (1 - a) * s<sub>i-1</sub> + a * x<sub>i</sub>
     *    w<sub>i</sub> = (1 - a) * w<sub>i-1</sub> + a
     *    
     *    s<sub>0</sub> = 0
     *    w<sub>0</sub> = 0
     *        
     * @see #update(double)
     * @see io.s4.zeno.statistics.Average#phantomGet(double)
     */
    public double phantomGet(double y) {
        double s1 = (1 - a) * s + a * y;
        double w1 = (1 - a) * w + a;

        return s1 / w1;
    }
}
