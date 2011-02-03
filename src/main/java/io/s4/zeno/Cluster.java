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

import io.s4.zeno.config.ConfigMap;
import io.s4.zeno.config.WritableConfigMap;
import io.s4.zeno.config.ZKConfigMap;

import java.util.List;


/**
 * Abstraction of a collection of Sites. Provides access to a list of names of
 * all sites in the cluster, and access to a read-only copy of the information
 * from all sites in the cluster.
 */
public interface Cluster {

    /**
     * Gets the information for a named node. The ConfigMap is backed by
     * ZooKeeper. Updates are applied to it asynchronously.
     * 
     * @see ZKConfigMap
     * 
     * @param name
     *            node's name
     * 
     * @return information for the node.
     */
    Cluster.Site getSite(String name);

    /**
     * Get names of all active sites in the cluster.
     * 
     * @return list of all active site names.
     */
    List<String> getAllSiteNames();

    /**
     * Get a list of all active sites in this cluster.
     * 
     * @return all active sites.
     */
    List<Site> getAllSites();

    /**
     * Add a Site to a cluster. The name of the site is obtained from
     * {@code site.name()}. A Writable config map is created for the site and
     * returned. Writes to this config map become visible to other Sites in the
     * cluster.
     * 
     * @param site
     * @return
     */
    WritableConfigMap addSite(Cluster.Site site);

    /**
     * Remove a site from the cluster.
     * 
     * @param site
     */
    void removeSite(Cluster.Site site);

    public interface Site {
        String name();

        ConfigMap info();
    }
}