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

import io.s4.zeno.resource.FlexibleResource;
import io.s4.zeno.resource.TimeSliceResource;
import io.s4.zeno.util.ZenoError;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;


/**
 * A mapping from Part ids to Parts. Functionality is provided for:
 *    1. acquiring a set of parts based on the job spec
 *    2. acquiring a set of parts based on a specified desired count
 *    3. identifying a collection of parts that consume a specified amount of resources
 *    4. adding to and removing from the mapping
 *    5. Looking up Parts corresponding to Ids 
 */
public class PartMap {

    private static final Logger logger = Logger.getLogger(PartMap.class);

    /**
     * Mapping from part id to part object. This is a concurrent HashMap to
     * handle the case where multiple threads modify the mapping.
     */
    protected ConcurrentHashMap<Part.Id, Part> parts = new ConcurrentHashMap<Part.Id, Part>();

    /**
     * Lookup part corresponding to a part id.
     * 
     * @param id
     *            part id
     * @return part corresponding to id, or null if no such part exists.
     */
    public Part get(Part.Id id) {
        return parts.get(id);
    }

    /**
     * Add a part to the mapping. Id is contained in the part.
     * 
     * @param part
     *            part to add
     */
    public void put(Part part) {
        logger.debug("adding partid " + part.id());
        parts.put(part.id(), part);
    }

    /**
     * Clear the mapping, without doing anything to the parts that are being
     * removed.
     */
    public void clear() {
        parts.clear();
    }

    /**
     * Forget a part. I.e. Remove its associated data from the node.
     * 
     * @param part
     *            part to forget
     */
    public void forget(Part part) {
        logger.debug("forgetting partid " + part.id());

        if (parts.remove(part.id()) != null) {
            part.forget();
        } else {
            throw new ZenoError("Unknown partid " + part.id());
        }
    }

    /**
     * Gets the number of parts contained in this map.
     * 
     * @return number of Part.Id to Part mappings contained in the map.
     */
    public int size() {
        return parts.size();
    }

    /**
     * Get list of all parts.
     * 
     * @return collection of all parts.
     */
    public Collection<Part> getAll() {
        return parts.values();
    }

    /**
     * Get the most busy parts that can fit into a flexible resource constraint.
     * The resource is expanded till some parts are identified to fit into it.
     * Expansion stops when the resource cannot be expanded further.
     * 
     * @param r
     *            flexible resource constraint into which parts have to be fit.
     * @return collection of busy parts. Collection is empty if no parts could
     *         be fit.
     */
    public Collection<Part> getBusy(FlexibleResource r) {
        ArrayList<Part> parts = new ArrayList<Part>(getAll());

        // order parts by resource usage: smallest first so that there is
        // greater granularity. THIS APPROACH HAS BEEN ABANDONED.
        // Collections.sort(parts, Collections.<Part>reverseOrder());

        // order parts by resource usage.
        Collections.sort(parts);

        ArrayList<Part> busy = new ArrayList<Part>();

        FlexibleResource free = r.duplicate();
        Resource totalUsed;

        expanding_loop: do {

            FlexibleResource oldFree = free.duplicate();
            totalUsed = new TimeSliceResource(0.0);

            logger.debug("trying to fit parts into " + free.toString());

            busy.clear();

            // fit subset into timeFraction
            for (Part p : parts) {
                if (free.almostEmpty()) break expanding_loop;

                Resource used = p.resourceUsage();
                logger.debug("tring partid " + p.id() + ": " + used);

                if (used.isEmpty()) continue; // we don't care about parts that
                                              // use no resources

                if (free.canAccept(used)) {
                    busy.add(p);
                    free.reduce(used);
                    totalUsed.add(used);
                }
            }

            if (oldFree.canExpand())
                free = oldFree.expand();
            else
                break;

        } while (true);

        // logger.debug("PARTS: " + parts);
        // logger.debug("BUSY: " + busy);

        logger.info("identified " + busy.size() + " parts to fill resources "
                + r + ". parts used: " + totalUsed);

        return busy;

    }

    /**
     * Freeze a set of parts. Parts are first paused. Then the method waits till
     * no event is sent to any of the parts for a period of time (i.e. the parts
     * are "silent"). If this fails for any part, then the whole operation is
     * rolled back.
     * <p>
     * When this method returns true, it is guaranteed that all the parts are
     * paused and have been silent for {@code silence} milliseconds. If this is
     * not the case, false is returned and the parts are unchanged; some of the
     * parts may have been paused and then resumed.
     * 
     * @param parts
     *            collection of parts
     * 
     * @param silence
     *            amount of time for which each part should be silent
     *            (milliseconds).
     * 
     * @param timeout
     *            total time after which the request times out.
     * 
     * @return true, if successful. In particular, false if waiting for silence
     *         on any part times out.
     */
    public static boolean freezeParts(Collection<Part> parts, long silence,
            long timeout) {

        // first pause each part
        for (Part part : parts) {
            part.pause();
        }

        // now wait till each part is silent
        try {
            for (Part part : parts)
                part.eventMonitor().waitForSilence(silence, timeout);

            // all parts have been silent long enough now.
            return true;

        } catch (InterruptedException e) {
            logger.info("interrupted waiting for silence", e);
        } catch (TimeoutException e) {
            logger.info("timed out waiting for silence", e);
        }

        // waiting for silence did not work!!

        // have to roll back pause for all parts.
        for (Part p : parts) {
            p.unpause();
        }

        // failed!
        return false;
    }
}
