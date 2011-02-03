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

import io.s4.zeno.EventMonitor;
import io.s4.zeno.Resource;
import io.s4.zeno.Service;
import io.s4.zeno.Site;

import org.apache.log4j.Logger;


/**
 * Advertise state of site.
 */
public class Advertiser extends Service {
    private static final Logger logger = Logger.getLogger(Advertiser.class);

    private Site site;

    public Advertiser(Site site) {
        this.site = site;
    }

    public void initialize() {
        //site.info() is guaranteed to be ready before service is started
        this.setInitialDelay(0, 5000);
        this.setDelay(5000);
    }

    public void action() {
        EventMonitor emon = site.eventMonitor();
        logger.info("load status: " + emon);

        if (emon.isValid()) {
            double rate = emon.getEventRate();
            double length = emon.getEventLength();

            site.info().set("load.eventRate", String.valueOf(rate));
            site.info().set("load.eventLength", String.valueOf(length));
        }

        Resource free = site.loadMonitor().getFreeResource();
        site.info().set("resource.free", free.toString());

        site.info().save();
    }

    public void cleanup() {
        site.info().remove("load.eventRate");
        site.info().remove("load.eventLength");
        site.info().remove("resource.free");
        site.info().save();
    }
}