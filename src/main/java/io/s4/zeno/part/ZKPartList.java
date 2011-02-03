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
package io.s4.zeno.part;

import io.s4.zeno.Job;
import io.s4.zeno.Part;
import io.s4.zeno.PartList;
import io.s4.zeno.Site;
import io.s4.zeno.util.ZenoDefs;
import io.s4.zeno.util.ZenoError;
import io.s4.zeno.util.ZooKeeperInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;


public class ZKPartList implements PartList {

    Logger logger = Logger.getLogger(ZKPartList.class);

    public ZKPartList(Site site, ZooKeeperInfo zkinfo) {
        this.site = site;
        this.zkinfo = zkinfo;
    }

    private final ZooKeeperInfo zkinfo;
    private final Site site;

    @Override
    public List<Part.Id> acquire(int n) {
        List<String> pnames = zkinfo.partsHolder.acquire(n, site.name());

        if (pnames.size() > 0) {
            ArrayList<Part.Id> acquired = new ArrayList<Part.Id>();

            for (String p : pnames) {
                Part.Id id = Part.Id.fromString(p);

                if (id != null) {
                    acquired.add(id);
                    logger.debug("acquired partid " + id);

                } else {
                    logger.error("could not interpret acquired part name as an Id, so releasing it: "
                            + p);
                    zkinfo.partsHolder.release(p);
                }
            }

            return acquired;

        } else {
            return Collections.<Part.Id> emptyList();
        }
    }

    @Override
    public void release(Part.Id id) {
        unmarkStarted(id);
        zkinfo.partsHolder.release(String.valueOf(id));
    }

    @Override
    public void markStarted(Part.Id id) {
        byte[] address = address(site).getBytes();
        String path = zkinfo.zkpath.routeMap(id.toString());

        try {
            zkinfo.zookeeper.create(path,
                                    address,
                                    ZenoDefs.zkACL,
                                    CreateMode.EPHEMERAL);
        } catch (Exception e) {
            throw new ZenoError("exception while marking as started partid "
                    + id, e);
        }
    }

    @Override
    public void unmarkStarted(Part.Id id) {
        String path = null;

        try {

            try {
                path = zkinfo.zkpath.routeHold(String.valueOf(id));
                zkinfo.zookeeper.delete(path, -1);
            } catch (KeeperException.NoNodeException e) {
                logger.debug("pause marker does not exist for partid " + id);
            }

            path = zkinfo.zkpath.routeMap(String.valueOf(id));
            zkinfo.zookeeper.delete(path, -1);

            return;

        } catch (Exception e) {
            throw new ZenoError("exception while unmarking as started partid "
                    + id, e);
        }
    }

    @Override
    public void markPaused(Part.Id id) {
        try {
            String path = zkinfo.zkpath.routeHold(String.valueOf(id));
            zkinfo.zookeeper.create(path,
                                    ZenoDefs.emptyBytes,
                                    ZenoDefs.zkACL,
                                    CreateMode.EPHEMERAL);

        } catch (KeeperException.NodeExistsException e) {
            logger.warn("trying to pause partid " + id
                    + " which maye have already been paused.");

        } catch (Exception e) {
            throw new ZenoError("exception while marking as paused partid "
                    + id, e);
        }
    }

    @Override
    public void unmarkPaused(Part.Id id) {
        try {
            String path = zkinfo.zkpath.routeHold(String.valueOf(id));
            zkinfo.zookeeper.delete(path, -1);

        } catch (Exception e) {
            throw new ZenoError("exception while unmarking as paused partid "
                    + id, e);
        }
    }

    private String address(Site site) {
        String host = site.info().get("IPAddress"); // <<<
        String port = site.info().get("port.event"); // <<<
        return host + ':' + port;
    }

    @Override
    public void markTakenOver(Part.Id id, Job job) {
        // Start with a part owned by another job
        // --- it is paused (by other job)
        //
        // 1. Take over ownership of this part.
        // --- if any failure occurs after this,
        // --- some other site will take over this part.
        // 2. Delete the route info for the part, keeping it paused.
        // 3. Create route info for the part, sending it to this site.
        // 4. Remove the pause marker.

        zkinfo.partsHolder.takeover(id.toString(), site.name().getBytes());

        String path = zkinfo.zkpath.routeMap(String.valueOf(id));

        try {
            try {
                zkinfo.zookeeper.delete(path, -1);
            } catch (KeeperException.NoNodeException e) {
                logger.warn("looks like "
                        + path
                        + " was deleted before transfer was completed. continuing.");
            }

            byte[] address = address(site).getBytes();
            zkinfo.zookeeper.create(path,
                                    address,
                                    ZenoDefs.zkACL,
                                    CreateMode.EPHEMERAL);
            logger.debug("set up route for partid " + id + ": " + path);

            // delete hold
            path = zkinfo.zkpath.routeHold(String.valueOf(id));

            try {
                zkinfo.zookeeper.delete(path, -1);
                logger.debug("deleted hold: " + path);
            } catch (KeeperException.NoNodeException e) {
                logger.warn("no hold was found at " + path
                        + " during takeover. continuing.");
            }

        } catch (KeeperException e) {
            throw new ZenoError("exception while taking over partid " + id, e);

        } catch (InterruptedException e) {
            throw new ZenoError("interrupted while taking over partid " + id, e);
        }
    }
}