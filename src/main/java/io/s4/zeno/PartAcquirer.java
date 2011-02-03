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
import io.s4.zeno.monitor.PoissonEventMonitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


/**
 * Logic for acquiring parts from a part list based on job configuration.
 */
public class PartAcquirer {
    private final static Logger logger = Logger.getLogger(PartAcquirer.class);

    // factory for generating monitors for each part
    private final EventMonitor.Factory monitorFactory;

    // Job with which this acquirer is associated.
    private final Job job;

    /**
     * Construct an acquirer in the context of a job.
     * 
     * @param job
     */
    public PartAcquirer(Job job) {
        this.job = job;

        // initialize monitor factory.
        ConfigMap partMonitorSpec = job.spec("part.monitor");
        monitorFactory = new PoissonEventMonitor.Factory(partMonitorSpec);
    }

    /**
     * Try to acquire a set of parts. The number of parts acquired is guaranteed
     * to be not more than {@code n}. The PartList associated with the site is
     * used to acquire the parts.
     * 
     * @param n
     *            number of parts desired.
     * 
     * @return list of Parts that have been acquired.
     * 
     */
    public List<Part> acquire(int n) {
        logger.debug("attempting to acquire " + n + " parts");

        List<Part.Id> partIds = job.site().partList().acquire(n);
        List<Part> parts = new ArrayList<Part>(partIds.size());

        for (Part.Id id : partIds) {
            logger.debug("adding partid " + id);

            EventMonitor monitor = monitorFactory.getInstance();

            Part part = new Part(job, monitor, id);

            parts.add(part);
        }

        logger.debug("acquired " + partIds.size() + " parts");

        return parts;

    }

    /**
     * Take over a part. This operation simply creates and returns a new
     * {@link Part.Taken} instance.
     * 
     * @param id
     *            identifier of part
     * @return new part instance.
     */
    public Part takeover(Part.Id id) {
        logger.debug("taking over partid " + id);

        EventMonitor monitor = monitorFactory.getInstance();
        return new Part.Taken(job, monitor, id);
    }
}
