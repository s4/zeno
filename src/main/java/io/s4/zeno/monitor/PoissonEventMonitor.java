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
package io.s4.zeno.monitor;

import io.s4.zeno.EventMonitor;
import io.s4.zeno.config.ConfigMap;
import io.s4.zeno.statistics.ExponentialMovingAverage;
import io.s4.zeno.statistics.PoissonEstimator;

import java.util.concurrent.TimeoutException;


/**
 * Estimate event arrival rate and average processing time assuming Poisson
 * arrivals.
 * <p>
 * If events arrive as a Poisson process with rate {@code r}, then the
 * inter-arrival times are exponentially distributed with mean {@code 1/r}.
 * PoissonEventMonitor estimates the rate {@code r} as the reciprocal of the
 * exponential moving average of the inter-arrival times.
 * <p>
 * The average processing time, {@code s}, is estimated as the exponential
 * moving average of the processing time.
 * <p>
 * For {@code n} events, numbered 1, 2, 3, ..., n:
 * 
 * <pre>
 *     r = 1 / EMA<sub>h</sub>(dT<sub>2</sub>, dT<sub>3</sub>, ..., dT<sub>n</sub>)
 *     s = EMA<sub>h</sub>(P<sub>1</sub>, P<sub>2</sub>, ..., P<sub>n</sub>)
 *     
 *     Where:
 *       * dT<sub>k</sub> = T<sub>k</sub> - T<sub>k-1</sub> 
 *       * T<sub>1</sub>, T<sub>2</sub>, ..., T<sub>n</sub> are the arrival times
 *       * P<sub>1</sub>, P<sub>2</sub>, ..., P<sub>n</sub> are the processing times
 *       * EMA<sub>h</sub> is the exponential moving average with half-life h
 * </pre>
 * 
 * The half-life, {@code h}, corresponds to the smallest number of trailing
 * observations whose cumulative weight exceeds 0.5.
 */
public class PoissonEventMonitor implements EventMonitor {

    /**
     * Factory class to construct multiple instances of PoissonEventMonitor with
     * the same half life.
     */
    public static class Factory implements EventMonitor.Factory {
        private final int halfLife;

        public Factory(int halfLife) {
            this.halfLife = halfLife;
        }

        public Factory(ConfigMap spec) {
            this.halfLife = spec.getInt("halfLife", 5); // $$
        }

        public EventMonitor getInstance() {
            return new PoissonEventMonitor(this.halfLife);
        }
    }

    // half-life
    private final int halfLife;

    // arrival rate
    private PoissonEstimator eventRate;

    // processing time.
    private ExponentialMovingAverage eventTime;

    /** The count. */
    private volatile int count = 0;

    /**
     * Instantiates a new poisson event monitor.
     * 
     * @param halfLife
     *            the half life
     */
    public PoissonEventMonitor(int halfLife) {
        this.halfLife = halfLife;
        initialize();
    }

    /**
     * Initialize.
     */
    private void initialize() {
        double refresh = (halfLife < 1 ? 1.0 : (1.0 - Math.pow(0.5,
                                                               1.0 / halfLife)));

        eventRate = new PoissonEstimator(refresh);
        eventTime = new ExponentialMovingAverage(refresh);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.EventMonitor#putEvent(double)
     */
    public void putEvent(double t) {
        eventRate.putEvent();
        eventTime.put(t);
        ++count;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.EventMonitor#getEventRate()
     */
    public double getEventRate() {
        return (count < 2 ? 0.0 : eventRate.getRate());
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.EventMonitor#getEventLength()
     */
    public double getEventLength() {
        return (count == 0 ? 0.0 : eventTime.get());
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.EventMonitor#isValid()
     */
    public boolean isValid() {
        return (count >= halfLife);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.EventMonitor#getMillisSinceLastEvent()
     */
    public long getMillisSinceLastEvent() {
        return eventRate.getMillisSinceLastEvent();
    }

    public void waitForSilence(long silenceMs, long timeoutMs)
            throws InterruptedException, TimeoutException {

        long remaining = timeoutMs;

        long s; // time remaining for request to be fulfilled.
        while ((s = silenceMs - getMillisSinceLastEvent()) > 0) {
            if ((remaining -= s) > 0L) // time should remain at end of wait
                Thread.sleep(s);

            else
                throw new TimeoutException("silenceMs=" + silenceMs
                        + ", timeoutMs=" + timeoutMs);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.EventMonitor#reset()
     */
    public void reset() {
        initialize();
        count = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        String message = "halfLife=" + halfLife + " count=" + count
                + " eventRate=" + getEventRate() + " eventLength="
                + getEventLength() + " busyFraction=" + getEventRate()
                * getEventLength();

        return message;
    }
}
