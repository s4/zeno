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
package io.s4.zeno.service;

import io.s4.zeno.Job;
import io.s4.zeno.LoadLevel;
import io.s4.zeno.Service;
import io.s4.zeno.Site;
import io.s4.zeno.config.ConfigMap;

import org.apache.log4j.Logger;


/**
 * If some parts are orphaned (i.e. not owned by any site), take some of them.
 */
public class PartAdopter extends Service {
    Logger logger = Logger.getLogger(PartAdopter.class);

    private Site site;

    public PartAdopter(Site site) {
        super("part-adopter");
        this.site = site;
    }

    @Override
    public void action() {
        loadParams();

        if (canAdoptPartitions()) {

            site.registry().tryLockAndRun("part_transfer", adoption);
        }
    }

    Runnable adoption = new Runnable() {
        public void run() {
            Job job = site.job();
            if (job == null) return;

            int n = job.acquireParts(adoptCount);

            if (n > 0) logger.info("adopted " + n + " partitions");
        }
    };

    public void initialize() {
        loadParams();
    }

    private int adoptCount = 0;

    public void loadParams() {
        Job job = site.job();
        if (job == null) return;

        ConfigMap spec = job.spec().chroot("part.adopt"); // <<<

        setDelay(spec.getInt("delay", 10000)); // 10 sec
        setInitialDelay(spec.getInt("initialDelay", 30000)); // 30 sec

        adoptCount = spec.getInt("count", 0);
    }

    // can this node adopt a partition?
    /**
     * Can adopt partitions.
     * 
     * @return true, if successful
     */
    private boolean canAdoptPartitions() {
        return (adoptCount > 0 && site.state() == Site.State.Running && (site.loadMonitor()
                                                                             .getLevel() != LoadLevel.High));
    }
}
