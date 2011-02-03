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
package io.s4.zeno.config;

import io.s4.zeno.util.ZenoError;
import io.s4.zeno.util.ZooKeeperHelper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;


// TODO: Auto-generated Javadoc
/**
 * The Class ZKConfigMap.
 */
public class ZKConfigMap extends JSONConfigMap {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(ZKConfigMap.class);

    /** The zookeeper. */
    protected ZooKeeperHelper zookeeper;

    /** The path. */
    protected String path;

    /** The z version. */
    protected volatile int zVersion;

    /** The updater. */
    protected Updater updater;

    /**
     * Instantiates a new zK config map.
     * 
     * @param zookeeper
     *            the zookeeper
     * @param path
     *            the path
     */
    public ZKConfigMap(ZooKeeperHelper zookeeper, String path) {

        logger.info("constructing zk-config for " + path);

        this.zookeeper = zookeeper;
        this.path = path;
        this.zVersion = -2;
        this.updater = new Updater();

        load();
    }

    /**
     * Load.
     * 
     * @return true, if successful
     */
    public boolean load() {
        try {

            logger.debug(this.hashCode() + ": loading from " + path);
            Stat stat = new Stat();

            if (zookeeper.exists(path, updater) != null) {
                // node exists: load config from there
                config = new JSONObject(new String(zookeeper.getData(path,
                                                                     updater,
                                                                     stat)));
                zVersion = stat.getVersion();

                logger.debug("version:" + zVersion + " config:" + config);

            } else {
                logger.debug("node does not exist. will load when it becomes available");
                config = null;

            }

            return true;

        } catch (KeeperException.NoNodeException e) {
            config = null;
            watchForCreation();
            logger.debug("node does not exist. will load when it becomes available: "
                    + e);
            return true;
        } catch (KeeperException e) {
            logger.error("caught exception: " + e);
            throw (new ZenoError("error loading config node " + path, e));

        } catch (InterruptedException e) {
            logger.error("interrupted");
        } catch (JSONException e) {
            logger.error("error parsing json: " + e);
        }

        // an exception has occurred.
        return false;
    }

    public void awaitUpdate(int initZVersion, int timeout)
            throws InterruptedException {
        synchronized (updater) {
            // wait for update only if zversion matches. i.e. an update is yet
            // to arrive.
            if (zVersion == initZVersion) updater.wait(timeout);
        }
    }

    /**
     * The Class Updater.
     */
    private class Updater implements Watcher {

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent
         * )
         */
        public void process(WatchedEvent e) {
            logger.debug("got notification: " + e);

            switch (e.getType()) {
                case NodeDeleted:
                    synchronized (this) {
                        config = null;
                        watchForCreation();
                        this.notifyAll();
                    }
                    break;

                case NodeCreated:
                case NodeDataChanged:
                    synchronized (this) {
                        load();
                        this.notifyAll();
                    }

                default:
                    // do nothing.
            }
        }
    }

    /**
     * Watch for creation.
     */
    private void watchForCreation() {
        try {
            // wait for creation
            logger.debug("watching for creation of " + path);
            zookeeper.exists(path, updater);

        } catch (KeeperException e) {
            logger.error("error while waiting for node to get created: " + e);
            throw (new ZenoError("error while watching for creation of config node "
                                         + path,
                                 e));

        } catch (InterruptedException e) {
            logger.error("interrupted while waiting for node to get created");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.JSONConfigMap#toString()
     */
    public String toString() {
        return zookeeper.toString() + "/" + path + ":" + super.toString();
    }
}
