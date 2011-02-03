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
import io.s4.zeno.Service;
import io.s4.zeno.Site;

import org.apache.log4j.Logger;


/**
 * Detect load level at site.
 */
public class LoadDetection extends Service {
    private static final Logger logger = Logger.getLogger(LoadDetection.class);

    Site site;
    
    public LoadDetection(Site site) {
        this.site = site;
    }
    
    protected void initialize() {
        setInitialDelay(0, 5000);
        setDelay(5000);
    }

    @Override
    protected void action() {
        LoadLevel level = site.loadMonitor().detectLevel();
        logger.debug(level);
    }
}
