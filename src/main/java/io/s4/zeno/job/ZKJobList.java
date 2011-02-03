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
package io.s4.zeno.job;

import io.s4.zeno.Job;
import io.s4.zeno.JobList;
import io.s4.zeno.Site;
import io.s4.zeno.config.ConfigMap;
import io.s4.zeno.config.ZKConfigMap;
import io.s4.zeno.coop.DistributedSequence;
import io.s4.zeno.coop.NonblockingLockset;
import io.s4.zeno.util.VolatileReference;
import io.s4.zeno.util.ZenoError;
import io.s4.zeno.util.ZooKeeperInfo;

import org.apache.log4j.Logger;


public class ZKJobList implements JobList {
    Logger logger = Logger.getLogger(ZKJobList.class);

    public ZKJobList(Site site, ZooKeeperInfo zkinfo) {
        this.site = site;
        this.zkinfo = zkinfo;
    }

    private final ZooKeeperInfo zkinfo;
    private final Site site;

    @Override
    public Job acquire() {
        logger.info("trying to acquire a job");

        // first try to get a task
        String jobName = zkinfo.taskHolder.acquireOne(site.name());

        // build and return the job
        return constructJob(jobName);
    }

    @Override
    public void release(Job job) {
        if (!zkinfo.taskHolder.release(job.name())) {
            throw new ZenoError("failed to release jobname " + job.name());
        }
    }

    @Override
    public Job standbyAcquire() {
        // jobName has to be final to be used by standbyAction...
        final VolatileReference<String> jobName = new VolatileReference<String>(null);

        DistributedSequence.Item standbyAction = new DistributedSequence.Item() {
            public byte[] getSequenceData() {
                return site.name().getBytes();
            }

            public void doHeadAction() {
                logger.debug("at head of standby queue");

                final NonblockingLockset taskHolder = zkinfo.taskHolder;
                final byte[] nodename = site.name().getBytes();

                // Try to get a job
                while ((jobName.v = taskHolder.acquireOne(nodename)) == null) {
                    logger.debug("failed to acquire a job. waiting for some change to happen.");
                    taskHolder.awaitUpdate();
                }

                logger.info("sequenced item got task " + jobName.v);
            }
        };

        DistributedSequence.SequencedItem item = zkinfo.standbySequence.add(standbyAction);

        logger.info("sequenced item to handle inclusion into active pool: "
                + item.toString());

        while (item.isActive()) {
            item.awaitDone();
        }

        if (jobName.v == null)
            throw new ZenoError("failed to acquire job after standing by.");

        return constructJob(jobName.v);
    }

    private Job constructJob(String name) {
        if (name != null) {
            ConfigMap spec = new ZKConfigMap(zkinfo.zookeeper,
                                             zkinfo.zkpath.task(name));

            return new Job(site, name, spec);

        } else {
            return null;
        }
    }
}
