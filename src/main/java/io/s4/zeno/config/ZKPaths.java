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
package io.s4.zeno.config;

// TODO: Auto-generated Javadoc
/**
 * The Class ZKPaths.
 */
public class ZKPaths {

    /** The zk base. */
    public final String zkBase;

    /** The task base. */
    public final String taskBase;

    /** The parts base. */
    public final String partsBase;

    /** The node base. */
    public final String nodeBase;

    /** The standby base. */
    public final String standbyBase;

    /** The resources base. */
    public final String resourcesBase;

    /** The route map base. */
    public final String routeMapBase;

    /** The route hold base. */
    public final String routeHoldBase;

    /** The shed. */
    public final String shed;

    /** The balance. */
    public final String balance;

    /**
     * Instantiates a new zK paths.
     * 
     * @param zkBase
     *            the zk base
     */
    public ZKPaths(String zkBase) {
        this.zkBase = zkBase;
        this.taskBase = zkBase + "/jobs";
        this.partsBase = zkBase + "/parts";
        this.nodeBase = zkBase + "/nodes/active";
        this.standbyBase = zkBase + "/nodes/standby";
        this.resourcesBase = zkBase + "/nodes/resources";
        this.routeMapBase = zkBase + "/route/map";
        this.routeHoldBase = zkBase + "/route/hold";
        this.shed = zkBase + "/transfer/shed";
        this.balance = zkBase + "/transfer/balance";
    }

    /**
     * Task.
     * 
     * @param s
     *            the s
     * @return the string
     */
    public String task(String s) {
        return taskBase + "/items/" + s;
    }

    /**
     * Part.
     * 
     * @param s
     *            the s
     * @return the string
     */
    public String part(String s) {
        return partsBase + "" +
        		"/items/" + s;
    }

    /**
     * Node.
     * 
     * @param s
     *            the s
     * @return the string
     */
    public String node(String s) {
        return nodeBase + '/' + s;
    }

    /**
     * Standby.
     * 
     * @param s
     *            the s
     * @return the string
     */
    public String standby(String s) {
        return standbyBase + '/' + s;
    }

    /**
     * Resource.
     * 
     * @param s
     *            the s
     * @return the string
     */
    public String resource(String s) {
        return resourcesBase + '/' + s;
    }

    /**
     * Route map.
     * 
     * @param s
     *            the s
     * @return the string
     */
    public String routeMap(String s) {
        return routeMapBase + '/' + s;
    }

    /**
     * Route hold.
     * 
     * @param s
     *            the s
     * @return the string
     */
    public String routeHold(String s) {
        return routeHoldBase + '/' + s;
        
    }
}
