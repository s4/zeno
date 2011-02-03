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
package io.s4.zeno;

import io.s4.zeno.resource.TimeSliceResource;
import io.s4.zeno.util.ZenoDefs;

import org.apache.log4j.Logger;


/**
 * A segment of events that are processed by a Job.
 * 
 * A Part is typically obtained from a PartList. Each Part measures its resource
 * usage.
 */
public class Part implements Comparable<Part> {
    private static Logger logger = Logger.getLogger(Part.class);

    public byte[] getData() {
        // TODO: extract data from site, related to this part
        return ZenoDefs.emptyBytes;
    }

    public Part(Job job, EventMonitor monitor, int group, int key) {
        this.monitor = monitor;
        this.job = job;
        this.id = new Id(group, key);
        this.state = State.Null;
    }

    public Part(Job job, EventMonitor monitor, Id id) {
        this.monitor = monitor;
        this.job = job;
        this.id = id.clone();
        this.state = State.Null;
    }

    public enum State {
        Null,
        Running,
        Paused,
        Invalid
    };

    State state;

    // State changes
    public void start() {
        if (state == State.Null) {
            job.site().partList().markStarted(id);
            state = State.Running;
        }
    }

    public void stop() {
        if (state == State.Running) {
            job.site().partList().unmarkStarted(id);
            state = State.Null;
        }
    }

    public void pause() {
        if (state == State.Running) {
            job.site().partList().markPaused(id);
            state = State.Paused;
        }
    }

    public void unpause() {
        if (state == State.Paused) {
            job.site().partList().unmarkPaused(id);
            state = State.Running;
        }
    }

    public void forget() {
    }

    // Identity
    public static class Id implements Cloneable, Comparable<Id> {
        public final int group;
        public final int key;

        public Id(int group, int key) {
            this.group = group;
            this.key = key;
        }

        public static Id fromString(String idStr) {
            String[] pieces = idStr.split(":");
            if (pieces.length == 2) {
                try {
                    int group = Integer.parseInt(pieces[0], 16);
                    int key = Integer.parseInt(pieces[1], 16);

                    return new Id(group, key);

                } catch (NumberFormatException e) {
                    logger.error("malformed hex number in Id: " + idStr, e);
                }
            } else {
                logger.error("incorrect number of parts in Id. "
                        + "expected GROUP:KEY (hex values); got: " + idStr);
            }

            return null;
        }

        public String toString() {
            return String.format("%08X:%08X", group, key);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Id) {
                Id that = (Id) o;
                return (this.group == that.group) && (this.key == that.key);
            }

            return false;
        }

        public int hashCode() {
            return (group << 16) | (key & 0x0000FFFF);
        }

        public int compareTo(Id that) {
            if (this.group != that.group)
                return (this.group < that.group ? -1 : +1);
            else if (this.key != that.key)
                return (this.key < that.key ? -1 : +1);
            else
                return 0;
        }

        public Id clone() {
            return new Id(group, key);
        }
    }

    Id id;

    public Id id() {
        return id;
    }

    // Monitor
    public final EventMonitor eventMonitor() {
        return monitor;
    }

    public Resource resourceUsage() {
        double usage = monitor.getEventLength() * monitor.getEventRate();
        return new TimeSliceResource(usage);
    }

    public String toString() {
        return id.toString();
    }

    // Order parts in decreasing order of resource usage
    public int compareTo(Part other) {
        return this.resourceUsage().compareTo(other.resourceUsage());
    }

    // Params
    protected final EventMonitor monitor;

    protected final Job job;

    // A taken-over part
    public static class Taken extends Part {
        Taken(Job job, EventMonitor monitor, Id id) {
            super(job, monitor, id);
        }

        private boolean takeoverDone = false;

        public void start() {
            if (takeoverDone)
                super.start();
            else
                takeoverStart();
        }

        protected void takeoverStart() {
            if (!takeoverDone) {
                // update routing information
                job.site().partList().markTakenOver(id, job);
                takeoverDone = true;
            }
        }
    }
}