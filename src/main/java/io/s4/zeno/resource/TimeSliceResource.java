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
package io.s4.zeno.resource;

import io.s4.zeno.EventMonitor;
import io.s4.zeno.Resource;

import org.apache.log4j.Logger;


// TODO: Auto-generated Javadoc
/**
 * The Class TimeSliceResource.
 */
public class TimeSliceResource implements Resource {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(TimeSliceResource.class);

    /** The time slice. */
    protected double timeSlice;

    /**
     * Instantiates a new time slice resource.
     * 
     * @param timeSlice
     *            the time slice
     */
    public TimeSliceResource(double timeSlice) {
        this.timeSlice = (timeSlice > 0.0 ? timeSlice : 0.0);
    }

    // type conversion from other resource types
    /**
     * Instantiates a new time slice resource.
     * 
     * @param r
     *            the r
     */
    public TimeSliceResource(Resource r) {
        if (r instanceof TimeSliceResource)
            this.timeSlice = ((TimeSliceResource) r).timeSlice;
        else
            this.timeSlice = 0.0;
    }

    /**
     * Gets the time slice.
     * 
     * @return the time slice
     */
    public double getTimeSlice() {
        return timeSlice;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.Resource#isEmpty()
     */
    public boolean isEmpty() {
        return this.timeSlice <= 0.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.Resource#canAccept(io.s4.zeno.Resource)
     */
    public boolean canAccept(Resource demand) {
        TimeSliceResource r = (TimeSliceResource) demand;
        return this.timeSlice > r.timeSlice;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.Resource#canAcceptPartial(io.s4.zeno.Resource)
     */
    public boolean canAcceptPartial(Resource demand) {
        return !isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.Resource#reduce(io.s4.zeno.Resource)
     */
    public void reduce(Resource use) {
        TimeSliceResource r = (TimeSliceResource) use;

        this.timeSlice -= r.timeSlice;
        if (this.timeSlice < 0) this.timeSlice = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.Resource#add(io.s4.zeno.Resource)
     */
    public void add(Resource use) {
        TimeSliceResource r = (TimeSliceResource) use;

        this.timeSlice += r.timeSlice;
    }

    // Natural order for Resources is: largest resource first
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Resource other) {
        TimeSliceResource r = (TimeSliceResource) other;
        if (this.timeSlice > r.timeSlice)
            return -1;
        else if (this.timeSlice < r.timeSlice)
            return +1;
        else {
            return (this.hashCode() > other.hashCode() ? -1 : +1);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return String.valueOf(timeSlice);
    }

    /**
     * From string.
     * 
     * @param s
     *            the s
     * @return the resource
     */
    public static Resource fromString(String s) {
        double ts = 0.0;

        try {
            if (s != null) ts = Double.valueOf(s);

        } catch (NumberFormatException e) {
            logger.error("malformed resource string: " + s);
            ts = 0.0;
        }

        return new TimeSliceResource(ts);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.Resource#toBytes()
     */
    public byte[] toBytes() {
        return toString().getBytes();
    }

    /**
     * From bytes.
     * 
     * @param data
     *            the data
     * @return the resource
     */
    public static Resource fromBytes(byte[] data) {
        return fromString((data != null ? (new String(data)) : null));
    }

    /**
     * The Class Capacity.
     */
    public static class Capacity {

        /** The c low. */
        private final double cLow;

        /** The c high. */
        private final double cHigh;

        /**
         * Instantiates a new capacity.
         * 
         * @param cLow
         *            the c low
         * @param cHigh
         *            the c high
         */
        public Capacity(double cLow, double cHigh) {
            this.cLow = cLow;
            this.cHigh = cHigh;
        }

        /**
         * Gets the excess.
         * 
         * @param monitor
         *            the monitor
         * @return the excess
         */
        public Resource getExcess(EventMonitor monitor) {
            double used = getUsage(monitor);
            double excess = (used < cHigh ? 0.0 : (used - cHigh));

            return new TimeSliceResource(excess);
        }

        /**
         * Gets the free.
         * 
         * @param monitor
         *            the monitor
         * @return the free
         */
        public Resource getFree(EventMonitor monitor) {
            double used = getUsage(monitor);
            double free = (used > cLow ? 0.0 : (cLow - used));

            return new TimeSliceResource(free);
        }

        /**
         * Gets the usage.
         * 
         * @param monitor
         *            the monitor
         * @return the usage
         */
        private double getUsage(EventMonitor monitor) {
            return monitor.getEventLength() * monitor.getEventRate();
        }
    }
}
