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
import io.s4.zeno.monitor.EventRateLoadMonitor;

/**
 * A Site at which a Job can run. The Site receives messages and processes them
 * based on the Job that it acquires.
 */
public class Site implements Cluster.Site {

    public synchronized void initialize() {
        if (state() == State.Null) {
            // Register myself with the cluster.
            info = cluster.addSite(this);

            // initialize the monitor
            monitor = new EventRateLoadMonitor(spec);

            if (initializer != null) {
                initializer.initialize(this);
            }

            state = State.Initialized;
        }
    }

    /**
     * Start the site and its registry.
     */
    public synchronized void start() {
        initialize();

        if (state() == State.Initialized) {

            // acquire a job
            job = jobList.acquire();

            if (job == null) {
                // job could not be acquired right away.
                // stand by for a job to become available.
                state = State.StandingBy;
                job = jobList.standbyAcquire();
            }

            // we should have a job by now.
            state = State.JobAcquired;

            registry.startServices();

            job.initialize();

            // All set now...
        }
    }

    /**
     * Stop the site.
     */
    public synchronized void stop() {
        if (state() == State.Running) {
            registry.stopServices();

            partMap.clear();

            job.release();
            job = null;

            cluster.removeSite(this);

            state = State.Null;
        }
    }

    public SiteRegistry registry() {
        return registry;
    }

    /**
     * Get the state of this site.
     * 
     * @return current state of the site.
     */
    public State state() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    /**
     * Get the load monitor associated with this site.
     * 
     * @return load monitor.
     */
    public LoadMonitor loadMonitor() {
        return monitor;
    }

    /**
     * Get the event monitor associated with this site. This is used to measure
     * the arrival rate and average length of events processed by this site.
     * 
     * @return event monitor.
     */
    public EventMonitor eventMonitor() {
        return monitor;
    }

    /**
     * Enumeration of possible states of a site.
     */
    public enum State {
        /**
         * Uninitialized.
         */
        Null,

        /**
         * Initialized.
         */
        Initialized,

        /**
         * Standing by to acquire a job.
         */
        StandingBy,

        /**
         * A job has been acquired.
         */
        JobAcquired,

        /**
         * Running the acquired job.
         */
        Running,

        /**
         * Job has been paused.
         */
        Paused,

        /**
         * Some error has occurred and the site is in an invalid state.
         */
        Invalid
    };

    /**
     * Node state
     */
    private State state = State.Null;

    /**
     * Combined monitor to measure load and event rate.
     */
    private EventRateLoadMonitor monitor = null;

    private PartMap partMap = null;

    // ///////////////////////////////////////
    // Cluster of which this Site is a member.
    private Cluster cluster = null;

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Cluster cluster() {
        return cluster;
    }

    // ///////////////////////////////////////
    // Status information for this node.
    private WritableConfigMap info = null;

    public WritableConfigMap info() {
        return info;
    }

    // ///////////////////////////////////////
    // Job running in this site.
    private Job job = null;
    
    public Job job() {
        return job;
    }

    // ///////////////////////////////////////
    // Job List. job is acquired from here.
    private JobList jobList = null;

    public JobList jobList() {
        return jobList;
    }

    public void setJobList(JobList jobList) {
        this.jobList = jobList;
    }

    // ///////////////////////////////////////
    // Part List
    private PartList partList = null;

    public PartList partList() {
        return partList;
    }

    public void setPartList(PartList partList) {
        this.partList = partList;
    }

    // ///////////////////////////////////////
    // Registry with services etc
    private SiteRegistry registry = new SiteRegistry();

    private String name = null;

    public String name() {
        return name;
    }

    // Specs of this site.
    private ConfigMap spec;

    public ConfigMap spec() {
        return spec;
    }

    // Initializer
    public interface Initializer {
        void initialize(Site site);
    }

    Initializer initializer = null;

    public void setInitializer(Initializer initializer) {
        this.initializer = initializer;
    }

    public Site(String name, ConfigMap spec) {
        this.name = name;
        this.spec = spec;
    }
}
