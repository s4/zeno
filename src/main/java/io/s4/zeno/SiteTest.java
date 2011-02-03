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

import io.s4.zeno.cluster.ZKCluster;
import io.s4.zeno.config.ConfigMap;
import io.s4.zeno.config.JSONConfigMap;
import io.s4.zeno.config.ZKPaths;
import io.s4.zeno.coop.DistributedSequence;
import io.s4.zeno.coop.NonblockingLockset;
import io.s4.zeno.job.ZKJobList;
import io.s4.zeno.part.ZKPartList;
import io.s4.zeno.route.Hasher;
import io.s4.zeno.route.ModuloHasher;
import io.s4.zeno.route.Router;
import io.s4.zeno.route.ZKRouter;
import io.s4.zeno.service.Advertiser;
import io.s4.zeno.service.Housekeeping;
import io.s4.zeno.service.LoadBalancer;
import io.s4.zeno.service.LoadDetection;
import io.s4.zeno.service.LoadShedder;
import io.s4.zeno.service.PartAdopter;
import io.s4.zeno.service.PartReceiver;
import io.s4.zeno.service.SimpleEventReceiver;
import io.s4.zeno.util.ZooKeeperHelper;
import io.s4.zeno.util.ZooKeeperInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONException;


public class SiteTest {

    static ZooKeeperHelper zookeeper;
    static ZKPaths zkpath;

    public static void main(String[] arg) throws JSONException,
            KeeperException, IOException, InterruptedException {

        PropertyConfigurator.configure("log4j.properties");

        String name = arg[0];
        String zkaddr = arg[1];
        String zkbase = arg[2];
        String specStr = arg[3];

        ConfigMap spec = new JSONConfigMap(specStr);

        ZooKeeper zk = new ZooKeeper(zkaddr, 3000, zkhandler);
        zookeeper = new ZooKeeperHelper(zk, 5, 5000);
        zkpath = new ZKPaths(zkbase);

        ZooKeeperInfo zkinfo = new ZooKeeperInfo();
        zkinfo.zookeeper = zookeeper;
        zkinfo.zkpath = zkpath;
        zkinfo.taskHolder = new NonblockingLockset(zookeeper, zkpath.taskBase);
        zkinfo.partsHolder = new NonblockingLockset(zookeeper, zkpath.partsBase);
        zkinfo.standbySequence = new DistributedSequence(zookeeper,
                                                         zkpath.standbyBase);

        final Site site = new Site(name, spec);

        ZKCluster cluster = new ZKCluster(zookeeper, zkpath);
        ZKJobList jobList = new ZKJobList(site, zkinfo);
        ZKPartList partList = new ZKPartList(site, zkinfo);
        SiteInitializer init = new SiteInitializer();

        site.setCluster(cluster);
        site.setJobList(jobList);
        site.setPartList(partList);
        site.setInitializer(init);

        class ZenoThreadGroup extends ThreadGroup {
            public ZenoThreadGroup() {
                super("ZenoThreadGroup");
            }

            public void uncaughtException(Thread t, Throwable e) {
                System.out.println("ZENOTHREADGROUP CAUGHT AND EXCEPTION FROM THREAD "
                        + t
                        + ". SO EXITING. DETAILS:"
                        + e
                        + "\nCAUSED BY: "
                        + e.getCause() + "\n");
                e.printStackTrace();

                System.exit(1);
            }
        }

        Thread t = new Thread(new ZenoThreadGroup(), new Runnable() {
            public void run() {
                site.start();
            }
        }, "zenorunthread");
        t.start();

        System.out.println();

        t.join();
    }

    public static class ZKHandler implements Watcher {
        public void process(WatchedEvent e) {
            System.out.println("RECEIVED NOTIFICATION FROM ZOOKEEPER: " + e);
        }
    }

    public static class SiteInitializer implements Site.Initializer {
        public void initialize(Site site) {

            site.info().set("IPAddress", getIPAddress());
            site.info().save();

            Service advertiser = new Advertiser(site);
            site.registry().registerService("advertiser", advertiser);

            Service adopter = new PartAdopter(site);
            site.registry().registerService("adopter", adopter);

            Hasher hasher = new ModuloHasher();
            Router router = new ZKRouter(zookeeper, zkpath, hasher);
            router.load();
            
            Service eventReceiver = new SimpleEventReceiver(site, hasher);
            site.registry().registerService("event-receiver", eventReceiver);

            Service partReceiver = new PartReceiver(site);
            site.registry().registerService("part-receiver", partReceiver);

            Service shedder = new LoadShedder(site);
            site.registry().registerService("load-shedder", shedder);

            Service balancer = new LoadBalancer(site, zookeeper, zkpath);
            site.registry().registerService("load-balancer", balancer);

            Service detect = new LoadDetection(site);
            site.registry().registerService("load-detection", detect);
            
            Service housekeeping  = new Housekeeping(site);
            site.registry().registerService("housekeeping", housekeeping);
        }

        private String getIPAddress() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "";
            }
        }

    }

    private static ZKHandler zkhandler = new ZKHandler();
}
