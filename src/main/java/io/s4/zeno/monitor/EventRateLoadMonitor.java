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
import io.s4.zeno.LoadLevel;
import io.s4.zeno.LoadMonitor;
import io.s4.zeno.Resource;
import io.s4.zeno.config.ConfigMap;
import io.s4.zeno.resource.TimeSliceResource;

import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;


/**
 * Load monitoring based on the rate at which events arrive and the amount of
 * time taken to process each event. The measure of resource usage is the
 * <i>fraction of wall-clock time</i> spent on processing events.
 * <p>
 * If events arrive at the rate of {@code r} per second and, on an average,
 * processing an event takes {@code s} seconds (wall-clock), then the fraction
 * of wall clock time spent processing events is defined as {@code B = r*s}.
 * This quantity can be greater than 1 in the case of multi-core machines.
 * <p>
 * This time fraction is represented as a {@link TimeSliceResource}. Two
 * thresholds: {@code eventHigh} and {@code eventLow} are used to decide if the
 * load level is {@code Low (B < eventLow)}, {@code High (B > eventHigh)}, or
 * {@code Medium (eventLow <= B <= eventHigh)}.
 * <p>
 * Two other thresholds, {@code accept} and {@code shed}, are used to determine
 * the free resources and excess resource utilization.
 * 
 * <pre>
 *     excessResorce = max {B-shed, 0}
 *     freeResource  = max {accept-B, 0}
 * </pre>
 */
public class EventRateLoadMonitor implements LoadMonitor, EventMonitor {

    private static final Logger logger = Logger.getLogger(EventRateLoadMonitor.class);

    // event rate estimator
    private final EventMonitor emon;

    // thresholds
    private final double eventLow;
    private final double eventHigh;
    private final double accept;
    private final double shed;

    // more thresholds for mem and cpu usage?

    // estimated load level
    private LoadLevel level = LoadLevel.Unknown;

    /**
     * Monitor with load level thresholds. This is identical to
     * {@code EventRateLoadMonitor(halfLife, eventLow, eventHigh, 1.0, 0.75, 0.85)}
     * 
     * @see #EventRateLoadMonitor(int, double, double, double, double, double)
     * 
     * @param halfLife
     *            half life for the rate estimator. See
     *            {@link PoissonEventMonitor}
     * @param eventLow
     *            low load threshold
     * @param eventHigh
     *            high load threshold
     */
    public EventRateLoadMonitor(int halfLife, double eventLow, double eventHigh) {
        emon = new PoissonEventMonitor(halfLife);
        this.eventLow = eventLow;
        this.eventHigh = eventHigh;
        this.accept = 0.75;
        this.shed = 0.85;
    }

    /**
     * Monitor with load level and resource availability thresholds.
     * 
     * @param halfLife
     *            half life for the rate estimator. See
     *            {@link PoissonEventMonitor}
     * @param eventLow
     *            low load threshold
     * @param eventHigh
     *            high load threshold
     * @param capacity
     *            wall-clock time fraction capacity. This is typically set to be
     *            equal to the number of available processor cores; in a 1
     *            second interval, a node with N cores can process events whose
     *            cumulative processing time is up to N seconds.
     * @param a
     *            fraction of capacity to be used as accept threshold.
     * @param s
     *            fraction of capacity to be used as shed threshold.
     */
    public EventRateLoadMonitor(int halfLife, double eventLow,
            double eventHigh, double capacity, double a, double s) {
        emon = new PoissonEventMonitor(halfLife);
        this.eventLow = eventLow;
        this.eventHigh = eventHigh;
        this.accept = capacity * a;
        this.shed = capacity * s;
    }

    /**
     * Initialize from a config map. The following fields from the config map
     * are used as parameters to
     * {@link #EventRateLoadMonitor(int, double, double, double, double, double)}
     * :
     * 
     * <pre>
     *     "monitor.halfLife"   halfLife
     *     "monitor.low"        eventLow
     *     "monitor.high"       eventHigh
     *     "resource.capacity"  capacity
     *     "resource.accept"    a
     *     "resource.shed"      s
     * </pre>
     * 
     * @param spec
     *            config map to initialize from
     */
    public EventRateLoadMonitor(ConfigMap spec) {
        this(spec.getInt("monitor.halfLife", 10), // <<<
             spec.getDouble("monitor.low", 0.5),
             spec.getDouble("monitor.high", 0.8),
             spec.getDouble("resource.capacity", 1.0),
             spec.getDouble("resource.accept", 0.75),
             spec.getDouble("resource.shed", 0.85));
    }

    public void putEvent(double t) {
        emon.putEvent(t);
    }

    public void reset() {
        level = LoadLevel.Unknown;
        emon.reset();
    }

    public long getMillisSinceLastEvent() {
        return emon.getMillisSinceLastEvent();
    }

    public void waitForSilence(long silenceMs, long timeoutMs)
            throws InterruptedException, TimeoutException {

        emon.waitForSilence(silenceMs, timeoutMs);
    }

    public double getEventRate() {
        return emon.getEventRate();
    }

    public double getEventLength() {
        return emon.getEventLength();
    }

    public boolean isValid() {
        return emon.isValid();
    }

    public LoadLevel getLevel() {
        return level;
    }

    public LoadLevel detectLevel() {
        double fracTime = getEventRate() * getEventLength();

        if (fracTime <= eventLow)
            level = LoadLevel.Low;
        else if (fracTime < eventHigh)
            level = LoadLevel.Medium;
        else
            level = LoadLevel.High;

        logger.debug("load level is " + level + " (busy=" + fracTime + " rate="
                + getEventRate() + " length=" + getEventLength() + ")");

        return level;
    }

    public Resource getResourceUsage() {
        double used = getEventRate() * getEventLength();
        return new TimeSliceResource(used);
    }

    public Resource getFreeResource() {
        double used = getEventRate() * getEventLength();
        return new TimeSliceResource(Math.max(0.0d, (accept - used)));
    }

    public Resource getExcessResourceUsage() {
        double used = getEventRate() * getEventLength();
        return new TimeSliceResource(Math.max(0.0d, (used - shed)));
    }

    public String toString() {
        return "LOADLEVEL:" + getLevel() + " " + emon.toString();
    }
}