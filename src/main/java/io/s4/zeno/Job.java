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
import io.s4.zeno.config.RootedConfigMap;

import java.util.List;

import org.apache.log4j.Logger;


/**
 * A Job that is scheduled to run at a Site. Starting the job results in a set
 * of Parts being acquired from a Site's PartList based on the specifications of
 * the Job. A Job is tightly coupled with a Site.
 */
public class Job {
    private static final Logger logger = Logger.getLogger(Job.class);

    public Job(Site site, String name, ConfigMap spec) {
        this.site = site;
        this.name = name;
        this.spec = spec;
    }

    /**
     * Initialize the job. This changes the state of the corresponding Site to
     * Running.
     */
    public void initialize() {

        // set up acquirer.
        partAcquirer = new PartAcquirer(this);

        // obtain initial set of parts.
        int nParts = spec.getInt("part.start.count", 0); // <<<
        int nAcq = acquireParts(nParts);
        
        logger.info("initialized with " + nAcq + " parts");

        site.setState(Site.State.Running);
    }

    private final String name;
    private final Site site;
    private final ConfigMap spec;
    private PartAcquirer partAcquirer;

    private PartMap partMap = new PartMap();

    // //////////////////////////////////////////////////////////
    // IDENTIFICATION and PROPERTIES ///////////////////////////
    // //////////////////////////////////////////////////////////

    /**
     * Gets the name.
     * 
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Get the job specification.
     * 
     * @return the job spec.
     */
    public ConfigMap spec() {
        return spec;
    }

    /**
     * Get the job spec, setting the root to {@code root}. All {@code get}s from
     * the resulting map are based relative to {@code root}.
     * 
     * @see RootedConfigMap
     * 
     * @param root
     *            Root of resulting map.
     * @return RootedConfigMap based at {@code root}.
     */
    public ConfigMap spec(String root) {
        return new RootedConfigMap(root, spec);
    }

    // //////////////////////////////////////////////////////////
    // Processing Node /////////////////////////////////////////
    // //////////////////////////////////////////////////////////

    /**
     * Gets the site in which this job is instantiated.
     * 
     * @return the processing node
     */
    public Site site() {
        return site;
    }

    // //////////////////////////////////////////////////////////
    // Stream Partitions ////////////////////////////////////////
    // //////////////////////////////////////////////////////////

    public PartMap partMap() {
        return partMap;
    }

    // //////////////////////////////////////////////////////////
    // Manipulate state of job /////////////////////////////////
    // //////////////////////////////////////////////////////////

    /**
     * Pause the job.
     * 
     * @return true, if successful
     */
    public synchronized void pause() {
        logger.info("pausing job " + name);

        // the action to be performed.
        Runnable action = new Runnable() {
            public void run() {
                if (site.state() == Site.State.Running) {
                    for (Part part : partMap().getAll())
                        part.pause(); // <<< pause each part
                    
                    site.setState(Site.State.Paused);
                }
            }
        };

        // make sure no transfer is in progress when this runs.
        site.registry().lockAndRun("part_transfer", action);
    }

    /**
     * Resume running the job.
     * 
     * @return true, if successful
     */
    public synchronized void unpause() {
        logger.info("unpausing job " + name);

        // the action to be performed.
        Runnable action = new Runnable() {
            public void run() {
                if (site.state() == Site.State.Paused) {
                    for (Part part : partMap().getAll())
                        part.unpause(); // <<< resume each part
                    
                    site.setState(Site.State.Running);
                }
            }
        };

        // make sure no transfer is in progress when this runs.
        site.registry().lockAndRun("part_transfer", action);
    }

    /**
     * Release the job. This makes it possible to acquire it from a JobList.
     */
    public synchronized void release() {
        Site.State s = site.state();
        logger.info("releasing job: " + name);

        if (s == Site.State.Running || s == Site.State.Paused) {
            logger.info("stopping all parts in job: " + name);

            Runnable action = new Runnable() {
                public void run() {
                    // from this point on, this site is not running a job.
                    site.setState(Site.State.Null);

                    // stop and release each part
                    for (Part part : partMap().getAll()) {
                        part.stop();
                    }

                    partMap().clear();

                    site.jobList().release(Job.this);
                }
            };

            // first grab the transfer lock
            site.registry().lockAndRun("part_transfer", action);
        }

    }

    public int acquireParts(int nParts) {
        List<Part> acquired = partAcquirer.acquire(nParts);
        for (Part p : acquired) {
            partMap.put(p);
            p.start();
        }
        
        return acquired.size();
    }

    public Part takeoverPart(Part.Id id) {
        // if this partid is already owned by this task, we are done.
        Part part = partMap.get(id);
        if (part != null) return part;

        // otherwise actually take it over
        part = partAcquirer.takeover(id);

        return part;
    }
}