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
import io.s4.zeno.Part;
import io.s4.zeno.PartMap;
import io.s4.zeno.Resource;
import io.s4.zeno.Site;
import io.s4.zeno.protocol.Connection;
import io.s4.zeno.resource.FlexibleResource;
import io.s4.zeno.resource.TimeSliceResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;


public class PartOffloader {
    private static final Logger logger = Logger.getLogger(PartOffloader.class);
    private Site site;

    public PartOffloader(Site site) {
        this.site = site;
    }

    /**
     * Use advertised free resources to find takers
     * 
     * @param wanted
     *            total resource wanted from takers
     * @return the list of takers
     */
    List<Cluster.Site> findTakers(Resource wanted) {
        logger.info("finding takers for excess resource: " + wanted);

        TreeMap<Resource, Cluster.Site> resourceMap = new TreeMap<Resource, Cluster.Site>();

        for (Cluster.Site rs : site.cluster().getAllSites()) {
            if (rs == site) continue; // skip "this" node

            Resource free = TimeSliceResource.fromString(rs.info()
                                                           .get("resource.free"));

            if (free.canAcceptPartial(wanted)) {
                resourceMap.put(free, rs);
                logger.info("otherNode " + rs.name() + " resources:" + free);
            }
        }

        return new ArrayList<Cluster.Site>(resourceMap.values());
    }

    /**
     * Identify and transfer some parts consuming a certain amount of resources
     * 
     * @param res
     *            FlexibleResource that has to be offloaded from this node.
     * @param nodeFreeReserve
     *            The minimum amount of resources that should be left free on
     *            each destination node.
     * @return number of partitions that were offloaded.
     */
    public int offload(FlexibleResource res, Resource nodeFreeReserve) {

        logger.info("trying to transfer some partitions to offload resources: "
                + res.toString());

        if (!res.isEmpty()) {
            // 1: find takers for the load
            List<Cluster.Site> takers = findTakers(res);

            logger.info("found " + takers.size() + " takers");

            if (!takers.isEmpty()) {
                // identify parts to shed
                Collection<Part> busyPartsCollection = site.job()
                                                           .partMap()
                                                           .getBusy(res);
                Iterator<Part> busyParts = busyPartsCollection.iterator();

                logger.info("identified " + busyPartsCollection.size()
                        + " parts to send");

                if (busyParts.hasNext()) {
                    Part currentPart = null;
                    OffloadPlan plan = new OffloadPlan();

                    // 1: offload some parts to each taker
                    for (Cluster.Site taker : takers) {
                        logger.info("trying taker: " + taker.name());

                        // connect
                        Connection conn = Connection.createTo(taker.info());
                        if (conn == null) continue;

                        logger.debug("connected");

                        // get latest free resource report
                        Sender sender = new Sender(conn);
                        if (!sender.hello(site.name())) {
                            logger.info(taker.name() + " rejected connection");
                            continue;
                        }

                        Resource free = sender.getFreeResource();

                        sender.goodbye();

                        if (nodeFreeReserve != null) {
                            // want to keep atleast nodeFreeReserve
                            // resources
                            // free on node.
                            // e.g. if total free resource is 80% and
                            // requested
                            // reserve is 20%
                            // => actually free = (80-20)% = 60%
                            free.reduce(nodeFreeReserve);
                        }

                        if (!free.isEmpty()) {

                            // send as many parts as we can fit into free
                            // space
                            while (busyParts.hasNext()) {
                                if (currentPart == null)
                                    currentPart = busyParts.next();

                                Resource used = currentPart.resourceUsage();

                                if (free.canAccept(used)) {
                                    plan.add(taker, currentPart);
                                    currentPart = null;

                                } else {
                                    logger.debug("reached capacity for this taker node.");
                                    break;
                                }
                            }
                        }
                    }

                    OffloadPlan remaining = plan.execute();

                    logger.info("remaining to offload: " + remaining.toString());
                    return plan.size() - remaining.size();
                }
            }
        }

        return 0;
    }

    /**
     * Offload partitions with no constraint on free resources on destination
     * nodes.
     * <p>
     * This is equivalent to
     * {@link #offloadPartitions(FlexibleResource, Resource)
     * offloadPartitions(res, null)}
     * 
     * @param res
     *            resources to shed from this node.
     */
    public int offload(FlexibleResource res) {
        return offload(res, null);
    }

    class OffloadPlan {
        // private final Logger logger = Logger.getLogger(OffloadPlan.class);

        HashMap<Cluster.Site, List<Part>> plan;

        private OffloadPlan(HashMap<Cluster.Site, List<Part>> plan) {
            this.plan = plan;
        }

        public OffloadPlan() {
            plan = new HashMap<Cluster.Site, List<Part>>();
        }

        public void add(Cluster.Site host, Part part) {
            List<Part> partList = plan.get(host);
            if (partList == null) {
                plan.put(host, (partList = new ArrayList<Part>()));
            }

            partList.add(part);
        }

        public List<Part> get(Cluster.Site host) {
            List<Part> ret = plan.get(host);

            return (ret != null ? ret : Collections.<Part> emptyList());
        }

        public List<Part> getAll() {
            List<Part> ret = new ArrayList<Part>();

            for (List<Part> l : plan.values()) {
                for (Part p : l) {
                    ret.add(p);
                }
            }

            return ret;
        }

        public int size() {
            int s = 0;
            for (List<Part> p : plan.values())
                s += p.size();

            return s;
        }

        public OffloadPlan execute() {
            HashMap<Cluster.Site, List<Part>> remaining = new HashMap<Cluster.Site, List<Part>>(plan);

            long silence = 5000;
            long timeout = 30000;
            if (PartMap.freezeParts(getAll(), silence, timeout)) {

                for (Cluster.Site taker : plan.keySet()) {
                    logger.info("sending to taker: " + taker.name());

                    // connect
                    Connection conn = Connection.createTo(taker.info());
                    if (conn == null) continue;

                    logger.debug("connected");

                    Sender sender = new Sender(conn);
                    
                    if (!sender.hello(site.name())) {
                        logger.info(taker.name() + " rejected connection");
                        continue;
                    }

                    int senderCount = 0;

                    List<Part> parts = get(taker);

                    List<Part> failed = new ArrayList<Part>();
                    for (Part p : parts) {
                        if (sender.sendPart(p)) {
                            // managed to send current part

                            logger.debug("sent part " + p.id());

                            site.job().partMap().forget(p);

                            ++senderCount;

                        } else {
                            logger.info("failed to send part: " + p.id());
                            failed.add(p);
                        }
                    }

                    sender.goodbye();

                    logger.info("sent " + senderCount + " parts to taker "
                            + taker.name() + ". " + failed.size() + " failed");

                    if (failed.size() == 0)
                        remaining.remove(taker);
                    else
                        remaining.put(taker, failed);
                }

            }

            return new OffloadPlan(remaining);
        }

        public String toString() {
            String s = "";
            for (Cluster.Site taker : plan.keySet()) {
                s += taker.name();
                s += ": { ";
                for (Part p : get(taker))
                    s += p.toString() + " ";
                s += "}\n";
            }
            return s;
        }
    }
}
