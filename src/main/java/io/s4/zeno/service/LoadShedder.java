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

import io.s4.zeno.LoadLevel;
import io.s4.zeno.Resource;
import io.s4.zeno.Service;
import io.s4.zeno.Site;
import io.s4.zeno.protocol.helper.PartOffloader;
import io.s4.zeno.resource.FlexibleResource;
import io.s4.zeno.resource.FlexibleTimeSliceResource;
import io.s4.zeno.resource.TimeSliceResource;

import org.apache.log4j.Logger;


/**
 * If load is high, shed some work to other nodes in the cluster.
 */
public class LoadShedder extends Service {
    private static final Logger logger = Logger.getLogger(LoadShedder.class);

    public LoadShedder(Site site) {
        this.site = site;
        offloader = new PartOffloader(site);
        setInitialDelay(30000, 60000);
        setDelay(30000);
    }

    private Site site;
    private PartOffloader offloader;

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.coop.BackgroundLoop#action()
     */
    public void action() {
        if ((site.loadMonitor().getLevel() == LoadLevel.High)) {
            logger.info("load is high. attempting to send some parts.");

            site.registry().lockAndRun("part_transfer", shedAction);
        }
    }
    
    Runnable shedAction = new Runnable() {
        public void run() {
            
            Resource excessExact = site.loadMonitor().getExcessResourceUsage();
            
            if (!(excessExact instanceof TimeSliceResource)) {
                logger.error("Load Shedder only works with TimeSlice resource units.");
                return;
            }
            
            final double marginLo = 0.1;
            final double marginHi = 0.2;
            final double expandLo = 1.2;
            final double expandHi = 1.5;
            final int expandCount = 5;
            
            FlexibleResource excess = new FlexibleTimeSliceResource((TimeSliceResource)excessExact,
                                                                    marginLo,
                                                                    marginHi,
                                                                    expandLo,
                                                                    expandHi,
                                                                    expandCount);

            logger.info("detected excess resource usage: " + excess);

            int sent = offloader.offload(excess);

            if (sent > 0) {
                site.eventMonitor().reset();
                logger.info("reset node event monitor after sending " + sent + " parts");
                
                site.registry().getActivityMonitor("part_transfer").tick();
            }
                    
        }
    };
}
