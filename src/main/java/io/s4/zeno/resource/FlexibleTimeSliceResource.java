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

import io.s4.zeno.Resource;

// TODO: Auto-generated Javadoc
/**
 * The Class FlexibleTimeSliceResource.
 */
public class FlexibleTimeSliceResource extends TimeSliceResource implements
        FlexibleResource {

    /** The expand count. */
    protected final int expandCount;

    /** The margin hi. */
    protected final double marginHi;

    /** The margin lo. */
    protected final double marginLo;

    /** The expand hi. */
    protected final double expandHi;

    /** The expand lo. */
    protected final double expandLo;

    /**
     * Instantiates a new flexible time slice resource.
     * 
     * @param r
     *            the r
     * @param marginLo
     *            the margin lo
     * @param marginHi
     *            the margin hi
     * @param expandLo
     *            the expand lo
     * @param expandHi
     *            the expand hi
     * @param expandCount
     *            the expand count
     */
    public FlexibleTimeSliceResource(Resource r, double marginLo,
            double marginHi, double expandLo, double expandHi, int expandCount) {
        this(new TimeSliceResource(r),
             marginLo,
             marginHi,
             expandLo,
             expandHi,
             expandCount);
    }

    /**
     * Instantiates a new flexible time slice resource.
     * 
     * @param r
     *            the r
     * @param marginLo
     *            the margin lo
     * @param marginHi
     *            the margin hi
     * @param expandLo
     *            the expand lo
     * @param expandHi
     *            the expand hi
     * @param expandCount
     *            the expand count
     */
    public FlexibleTimeSliceResource(TimeSliceResource r, double marginLo,
            double marginHi, double expandLo, double expandHi, int expandCount) {
        this(r.getTimeSlice(),
             marginLo,
             marginHi,
             expandLo,
             expandHi,
             expandCount);
    }

    /**
     * Instantiates a new flexible time slice resource.
     * 
     * @param timeSlice
     *            the time slice
     * @param marginLo
     *            the margin lo
     * @param marginHi
     *            the margin hi
     * @param expandLo
     *            the expand lo
     * @param expandHi
     *            the expand hi
     * @param expandCount
     *            the expand count
     */
    public FlexibleTimeSliceResource(double timeSlice, double marginLo,
            double marginHi, double expandLo, double expandHi, int expandCount) {
        this(timeSlice,
             marginLo,
             marginHi,
             expandLo,
             expandHi,
             expandCount,
             false);
    }

    /**
     * Instantiates a new flexible time slice resource.
     * 
     * @param timeSlice
     *            the time slice
     * @param marginLo
     *            the margin lo
     * @param marginHi
     *            the margin hi
     * @param expandLo
     *            the expand lo
     * @param expandHi
     *            the expand hi
     * @param expandCount
     *            the expand count
     * @param absolute
     *            the absolute
     */
    public FlexibleTimeSliceResource(double timeSlice, double marginLo,
            double marginHi, double expandLo, double expandHi, int expandCount,
            boolean absolute) {
        super(timeSlice);
        this.marginHi = (absolute ? marginHi : (marginHi * timeSlice));
        this.marginLo = (absolute ? marginLo : (marginLo * timeSlice));
        this.expandHi = expandHi;
        this.expandLo = expandLo;
        this.expandCount = expandCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.resource.FlexibleResource#canExpand()
     */
    public boolean canExpand() {
        return (expandCount > 0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.resource.FlexibleResource#expand()
     */
    public FlexibleResource expand() {
        if (canExpand()) {
            return new FlexibleTimeSliceResource(((TimeSliceResource) this).getTimeSlice(),
                                                 marginLo * expandLo,
                                                 marginHi * expandHi,
                                                 expandLo,
                                                 expandHi,
                                                 (expandCount - 1),
                                                 true);
        }

        return this.duplicate();

    }

    // create a duplicate
    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.resource.FlexibleResource#duplicate()
     */
    public FlexibleResource duplicate() {
        return new FlexibleTimeSliceResource(((TimeSliceResource) this).getTimeSlice(),
                                             marginLo,
                                             marginHi,
                                             expandLo,
                                             expandHi,
                                             expandCount,
                                             true);
    }

    /**
     * Lo.
     * 
     * @return the double
     */
    private double lo() {
        return timeSlice - marginLo;
    }

    /**
     * Hi.
     * 
     * @return the double
     */
    private double hi() {
        return timeSlice + marginHi;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * io.s4.zeno.resource.TimeSliceResource#canAcceptPartial(com.yahoo.
     * zeno.Resource)
     */
    public boolean canAcceptPartial(Resource demand) {
        // TimeSliceResource r = (TimeSliceResource)demand;
        return this.hi() > 0.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * io.s4.zeno.resource.TimeSliceResource#canAccept(io.s4.zeno.Resource
     * )
     */
    public boolean canAccept(Resource demand) {
        TimeSliceResource r = (TimeSliceResource) demand;
        return this.hi() > r.timeSlice;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.resource.TimeSliceResource#isEmpty()
     */
    public boolean isEmpty() {
        return this.hi() <= 0.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.resource.FlexibleResource#almostEmpty()
     */
    public boolean almostEmpty() {
        return this.lo() <= 0.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.resource.TimeSliceResource#toString()
     */
    public String toString() {
        return super.toString() + ":[-" + marginLo + "(x" + expandLo + "), +"
                + marginHi + "(x" + expandHi + "); " + expandCount + "X]";
    }
}
