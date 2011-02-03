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
 * Estimate event arrival rate Poisson arrivals.
 * <p>
 * If events arrive as a Poisson process with rate {@code r}, then the
 * inter-arrival times are exponentially distributed with mean {@code 1/r}.
 * PoissonEventMonitor estimates the rate {@code r} as the reciprocal of the
 * exponential moving average of the inter-arrival times.
 * <p>
 * For {@code n} events, numbered 1, 2, 3, ..., n:
 * 
 * <pre>
 *     r = 1 / EMA<sub>h</sub>(dT<sub>2</sub>, dT<sub>3</sub>, ..., dT<sub>n</sub>)
 *     
 *     Where:
 *       * dT<sub>k</sub> = T<sub>k</sub> - T<sub>k-1</sub> 
 *       * T<sub>1</sub>, T<sub>2</sub>, ..., T<sub>n</sub> are the arrival times
 *       * EMA<sub>h</sub> is the exponential moving average with refresh parameter h
 * </pre>
 * 
 * The refresh parameter {@code h} is the weight of the last element in the
 * sequence. See {@link ExponentialMovingAverage} for how this parameter relates
 * to the half-life of the average.
 */
public class PoissonEstimator implements RateEstimator {

    private final Average avgT;

    private long last = 0;

    /**
     * Constructor
     * 
     * @param refresh
     *            refresh parameter of underlying exponential moving average.
     */
    public PoissonEstimator(double refresh) {
        avgT = new ExponentialMovingAverage(refresh);
        last = System.currentTimeMillis();
    }

    public void putEvent() {
        long now = System.currentTimeMillis();

        avgT.put(now - last);

        last = now;
    }

    public double getRate() {
        if (!avgT.empty()) {
            long now = System.currentTimeMillis();

            // pretend that an event occurred at this time. So call
            // phantomGet(...)
            return 1000.0 / avgT.phantomGet(now - last);
        }

        return 0.0;
    }

    public long getMillisSinceLastEvent() {
        if (!avgT.empty()) {
            long now = System.currentTimeMillis();
            return (now - last);
        }

        return Long.MAX_VALUE;
    }
}
