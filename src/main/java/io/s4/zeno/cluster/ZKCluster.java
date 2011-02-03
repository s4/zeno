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
package io.s4.zeno.cluster;

import io.s4.zeno.Cluster;
import io.s4.zeno.config.ConfigMap;
import io.s4.zeno.config.WritableConfigMap;
import io.s4.zeno.config.ZKConfigMap;
import io.s4.zeno.config.ZKPaths;
import io.s4.zeno.config.ZKWritableConfigMap;
import io.s4.zeno.util.ZenoError;
import io.s4.zeno.util.ZooKeeperHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;


/**
 * View of ZooKeeper-managed cluster from a particular node within the cluster.
 * 
 * TODO: Watch for changes to list of nodes. If that happens, update mapping
 * automatically. Then don't call getChildren for each call to getAllNodeInfo.
 */
public class ZKCluster implements Cluster {
    private static final Logger logger = Logger.getLogger(ZKCluster.class);

    private static class RemoteSite implements Cluster.Site {
        public String name;
        public ConfigMap info;

        public RemoteSite(String name, ZKConfigMap info) {
            this.name = name;
            this.info = info;
        }

        public String name() {
            return name;
        }

        public ConfigMap info() {
            return info;
        }
    }

    private ZooKeeperHelper zookeeper;

    private ZKPaths zkpath;

    /** Mapping from node name to its information. */
    private ConcurrentHashMap<String, Cluster.Site> siteMap = new ConcurrentHashMap<String, Cluster.Site>();

    /**
     * Create a cluster object where all nodes' information is read-only.
     * 
     * @see #ZKClusterOld(ZooKeeperHelper, ZKPaths, ProcessingNode)
     * 
     * @param zookeeper
     *            Zookeeper client
     * @param zkpath
     *            Set of paths to various zeno elements in zookeeper
     */
    public ZKCluster(ZooKeeperHelper zookeeper, ZKPaths zkpath) {
        this.zookeeper = zookeeper;
        this.zkpath = zkpath;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.cluster.Cluster#getNodeInfo(java.lang.String)
     */
    public Site getSite(String name) {
        Site site = siteMap.get(name);
        if (site != null) return site;

        ZKConfigMap newInfo = new ZKConfigMap(zookeeper, zkpath.node(name));
        Site newSite = new RemoteSite(name, newInfo);

        site = siteMap.putIfAbsent(name, newSite);

        return (site == null ? newSite : site);
    }

    public List<String> getAllSiteNames() {
        logger.debug("loading all active sites");

        try {
            List<String> allNodes = zookeeper.getChildren(zkpath.nodeBase,
                                                          false);

            return allNodes;

        } catch (KeeperException e) {
            logger.error("error while constructing list of node information: "
                    + e);
        } catch (InterruptedException e) {
            logger.error("interrupted while constructing list of node information: "
                    + e);
        }

        return Collections.<String> emptyList();
    }

    public List<Site> getAllSites() {
        List<String> allNames = getAllSiteNames();
        ArrayList<Site> allSites = new ArrayList<Site>(allNames.size());
        for (String name : allNames) {
            allSites.add(getSite(name));
        }

        return allSites;
    }

    public WritableConfigMap addSite(Site site) {
        String name = site.name();

        ZKWritableConfigMap info = new ZKWritableConfigMap(zookeeper,
                                                           zkpath.node(name),
                                                           ZKWritableConfigMap.Mode.NoAutoSave);

        // this node may already exist. create is safe in that case: it returns
        // false. But we don't really care about that case.
        info.create();

        siteMap.put(name, site);

        return info;
    }

    public void removeSite(Site site) {
        String name = site.name();

        // remove it from the local cache.
        siteMap.remove(name);

        try {
            // delete any existing version of this znode.
            zookeeper.delete(zkpath.node(name), -1);

        } catch (KeeperException.NoNodeException e) {
            logger.error("failed to remove site " + name
                    + ". znode does not exist.", e);
        } catch (InterruptedException e) {
            logger.error("interrupted while trying to remove site from cluster.",
                         e);
        } catch (KeeperException e) {
            throw new ZenoError("error while removing site " + name
                    + " from cluster.", e);
        }
    }
}
