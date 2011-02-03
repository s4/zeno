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
package io.s4.zeno.protocol.helper;

import io.s4.zeno.Cluster;
import io.s4.zeno.LoadLevel;
import io.s4.zeno.Resource;
import io.s4.zeno.Site;
import io.s4.zeno.resource.FlexibleResource;
import io.s4.zeno.resource.FlexibleTimeSliceResource;
import io.s4.zeno.resource.TimeSliceResource;
import io.s4.zeno.service.LoadBalancer;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * Balance by reassigning partitions to make TimeSlice resource usage of each
 * node close to the average across the cluster.
 * 
 * @see LoadBalancer
 */
public class TimeSliceBalancer {

    private static final Logger logger = Logger.getLogger(TimeSliceBalancer.class);

    private final Site site;
    private final PartOffloader offloader;

    public TimeSliceBalancer(Site site) {
        this.site = site;
        offloader = new PartOffloader(site);
    }

    /**
     * Perform balancing
     */
    public void doBalance() {
        // Precondition for balancing:
        // 0. Node has recovered from previous transfers.
        // 1. All nodes have some advertised free resources.
        // We don't want to get in the way of load shedding.
        // 2. This node does not have High load.
        // 3. This node is not experiencing Low load.
        // Balancing is not necessary for low load nodes. Combining
        // with (1),(2), this means load level must be medium.
        // 4. Free resources on this node is below average.

        // Postcondition after balancing:
        // 0. L.B. does not run
        // 0.a. Cluster is in the realm of load shedding: some node has
        // no
        // advertised free resources, or
        // 0.b. This node is in the realm of load shedding: experiencing
        // high load, or
        // 0.c. Load level is low, or
        // 0.d. Node is recovering from a transfer, or
        // 1. This node's free resources are within a margin of average.

        if (!site.registry().getActivityMonitor("part_transfer").isSilent(5000)) {
            logger.info("not balancing. node is recovering from a previous transfer");
            return;
        } else if (site.loadMonitor().detectLevel() != LoadLevel.Medium) {
            logger.debug("not balancing. load level is "
                    + site.loadMonitor().getLevel());
            return;
        }

        // // Get info for all nodes
        List<Cluster.Site> allSites = site.cluster().getAllSites();

        int n = 0;
        double s = 0.0;
        double thisFree = 0.0;

        boolean balanceRequired = true;

        HashMap<String, Double> nodeFree = new HashMap<String, Double>();

        for (Cluster.Site rs : allSites) {
            double f = rs.info().getDouble("resource.free", -1.0);

            logger.debug("advertised free resources for " + rs.name() + ": "
                    + f);

            if (f > 0.0) {
                s += f;
                ++n;

                if (rs == site)
                    thisFree = f;
                else
                    nodeFree.put(rs.name(), f);

            } else if (f <= 0.0) {
                balanceRequired = false;
                break;
            }
        }

        if (!balanceRequired) {
            logger.info("not balancing. some nodes have no free resources. will not get in the way of load shedding.");
            return;
        }

        double average = s / n;

        logger.info("this node free resources: " + thisFree);
        logger.info("average  free  resources: " + average);

        // nothing to do if this node has more free resources than
        // average using a "hysteresis factor"
        final double margin = 0.20;
        if (thisFree >= average * (1 - margin)) {
            logger.info("not balancing. free resources are close to or higher than average.");
            return;
        }

        // otherwise, go over all other nodes and send some load to
        // them.
        logger.info("free resource is below average.");

        final double marginLo = margin;
        final double marginHi = margin;
        final double expandLo = 1.2;
        final double expandHi = 1.2;
        final int expandCount = 5;

        final FlexibleResource excess = new FlexibleTimeSliceResource(average
                - thisFree, marginLo, marginHi, expandLo, expandHi, expandCount);

        final Resource nodeFreeReserve = new TimeSliceResource(average * (1 - margin));

        // do a balance action only if we can readily acquire the lock.
        // don't block
        Runnable balanceAction = new Runnable() {
            public void run() {
                int sent = offloader.offload(excess, nodeFreeReserve);

                logger.info("offloaded " + sent + " partitions");

                if (sent > 0) {
                    site.eventMonitor().reset();
                    site.registry().getActivityMonitor("part_transfer").tick();
                }

            }
        };

        if (!site.registry().tryLockAndRun("part_transfer", balanceAction)) {
            logger.info("can't run a balance action. Another transfer is in progress.");
        }
    }

}