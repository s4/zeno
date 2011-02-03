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

import io.s4.zeno.Service;
import io.s4.zeno.Site;
import io.s4.zeno.config.ZKPaths;
import io.s4.zeno.coop.DistributedSequence;
import io.s4.zeno.protocol.helper.TimeSliceBalancer;
import io.s4.zeno.util.ZooKeeperHelper;

import org.apache.log4j.Logger;


/**
 * If necessary, equalize load by shedding whatever is in excess of the cluster
 * average.
 */
public class LoadBalancer extends Service {
    private static final Logger logger = Logger.getLogger(LoadBalancer.class);

    /**
     * Queue of balance actions. Only one action can be performed at a time.
     */
    DistributedSequence balanceQueue;
    TimeSliceBalancer balancer;
    Site site;

    public LoadBalancer(Site site, ZooKeeperHelper zookeeper, ZKPaths zkpath) {
        this.site = site;
        balanceQueue = new DistributedSequence(zookeeper, zkpath.balance);
        /** Balancer algorithm. */
        balancer = new TimeSliceBalancer(site);

        setInitialDelay(30000, 60000);
        setDelay(30000);
    }

    /** A sequenced balance action. */
    DistributedSequence.SequencedItem sequenced = null;

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.coop.BackgroundLoop#action()
     */
    protected void action() {
        if ((sequenced == null || sequenced.isDone())) {
            // no pending balance action?
            // add one to sequence

            DistributedSequence.Item balanceAction = new DistributedSequence.Item() {
                public byte[] getSequenceData() {
                    return site.name().getBytes();
                }

                public void doHeadAction() {
                    balancer.doBalance();
                }
            };

            logger.debug("sequencing a balance action for " + site.name());

            sequenced = balanceQueue.add(balanceAction);

            logger.debug("sequenced a balance action for " + site.name());
        }
    }

}
