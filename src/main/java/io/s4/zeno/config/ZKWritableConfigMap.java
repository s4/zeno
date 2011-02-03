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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;


// TODO: Auto-generated Javadoc
/**
 * The Class ZKWritableConfigMap.
 */
public class ZKWritableConfigMap extends ZKConfigMap implements
        WritableConfigMap {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(ZKWritableConfigMap.class);

    /** The Constant emptyJsonBytes. */
    private static final byte[] emptyJsonBytes = (new String("{}")).getBytes();

    /** The addset. */
    private volatile ConcurrentHashMap<String, String> addset = new ConcurrentHashMap<String, String>();

    // this is really just a set.
    // But java.util.concurrent only provides a concurrent map :(
    private volatile ConcurrentHashMap<String, Integer> removeset = new ConcurrentHashMap<String, Integer>();

    // NO COPY CONSTRUCTOR. CANNOT MAKE A COPY.
    // private ZKWritableConfigMap(ZKWritableConfigMap x) {}

    /**
     * Instantiates a new zK writable config map.
     * 
     * @param zookeeper
     *            the zookeeper
     * @param path
     *            the path
     * @param mode
     *            the mode
     */
    public ZKWritableConfigMap(ZooKeeperHelper zookeeper, String path, Mode mode) {
        super(zookeeper, path);

        this.mode = mode;
    }

    /**
     * Instantiates a new zK writable config map.
     * 
     * @param zookeeper
     *            the zookeeper
     * @param path
     *            the path
     */
    public ZKWritableConfigMap(ZooKeeperHelper zookeeper, String path) {
        this(zookeeper, path, Mode.AutoSave);
    }

    /**
     * The Enum Mode.
     */
    public enum Mode {

        /** The Auto save. */
        AutoSave,

        /** The No auto save. */
        NoAutoSave
    }

    /** The mode. */
    private final Mode mode;

    /**
     * Creates the.
     * 
     * @return true, if successful
     */
    public boolean create() {
        try {

            if (zookeeper.exists(path, false) == null)
                zookeeper.create(path,
                                 emptyJsonBytes,
                                 ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.EPHEMERAL);

            load();
            return true;

        } catch (KeeperException.NodeExistsException e) {
            logger.info("config node already exists: " + e);
        } catch (KeeperException e) {
            logger.error("caught exception: " + e);
            throw (new ZenoError("error creating config node " + path, e));

        } catch (InterruptedException e) {
            logger.error("interrupted");
        }

        return false;
    }

    /**
     * Delete.
     * 
     * @return true, if successful
     */
    public boolean delete() {
        try {

            zookeeper.delete(path, -1);
            return true;

        } catch (KeeperException e) {
            logger.error("caught exception: " + e);
            throw (new ZenoError("error deleting config node " + path, e));

        } catch (InterruptedException e) {
            logger.error("interrupted");
        }

        return false;
    }

    // multiple set/removes are possible, but cannot run concurrently with a
    // save.
    ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.WritableConfigMap2#set(java.lang.String,
     * java.lang.String)
     */
    @Override
    public boolean set(String key, String value) {
        rwl.readLock().lock();

        try {
            addset.put(key, value);

        } finally {
            rwl.readLock().unlock();
        }

        return commit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.WritableConfigMap2#remove(java.lang.String)
     */
    @Override
    public boolean remove(String key) {
        rwl.readLock().lock();

        try {
            removeset.putIfAbsent(key, 1);
            addset.remove(key);

        } finally {
            rwl.readLock().unlock();
        }

        return commit();
    }

    /**
     * Commit.
     * 
     * @return true, if successful
     */
    private boolean commit() {
        switch (mode) {
            case NoAutoSave:
                return true;

            default:
                return save();
        }
    }

    final int retries = 10;
    final int retryTimeout = 2000; // 2 sec timeout between retries

    /**
     * Save.
     * 
     * @return true, if successful
     */
    public boolean save() {

        logger.debug("saving...");

        rwl.writeLock().lock();

        // try-finally to release write lock.
        try {
            if (clean()) {
                logger.debug("no changes to save");
                return true;
            }

            logger.debug("old-config:" + config);
            logger.debug("addset:" + addset);
            logger.debug("removeset:" + removeset);

            int r = 0;

            do {
                JSONObject c = mergedConfig();
                if (c == null) return false;

                logger.debug("new-config: " + c);

                try {
                    int myVersion = zVersion;

                    logger.debug("loaded-version: " + myVersion);

                    Stat stat = zookeeper.exists(path, updater);

                    if (stat == null) {
                        logger.warn("config was removed!");
                        return false;
                    }

                    if (writeToZooKeeper(c, stat)) return true;

                    // the write may fail because some other client
                    // updated the config.

                    logger.info("waiting for update before retrying.");
                    awaitUpdate(myVersion, retryTimeout);

                } catch (InterruptedException e) {
                    logger.warn("interrupted while writing config to zookeeper",
                                e);
                    return false;

                } catch (KeeperException e) {
                    throw (new ZenoError("error saving config in " + path, e));
                }

            } while (++r <= retries);

            // out of retries
            logger.error("could not save after " + r + " retries");
            return false;

        } finally {
            rwl.writeLock().unlock();
        }
    }

    private boolean clean() {
        // no pending adds or removes
        return addset.isEmpty() && removeset.isEmpty();
    }

    private JSONObject mergedConfig() {
        JSONObject c = config;

        if (c != null) {
            // update only if the node has not been deleted.

            try {
                for (String k : removeset.keySet())
                    c.remove(k);

                for (Map.Entry<String, String> e : addset.entrySet())
                    c.put(e.getKey(), e.getValue());

                return c;

            } catch (JSONException e) {
                throw new ZenoError("problem merging addset, removeset into config.",
                                    e);
            }
        }

        // config is null
        return null;
    }

    /**
     * Write config to zookeeper. Returns true if successful, false if the
     * config was concurrently updated by another client. Throws a ZenoError if
     * some zookeeper system error occurs.
     * 
     * @param c
     *            JSON config to save
     * @return true if successful. False if retry should happen.
     * @throws InterruptedException
     */
    private boolean writeToZooKeeper(JSONObject c, Stat stat)
            throws InterruptedException, KeeperException {

        int pathVersion = stat.getVersion();
        logger.debug("current-version: " + pathVersion);

        // should we wait for an update to happen?
        if (pathVersion > zVersion) return false;

        byte[] data = c.toString().getBytes();

        try {
            zookeeper.setData(path, data, zVersion);

            addset.clear();
            removeset.clear();

            logger.debug("saved");

            return true;

        } catch (KeeperException.BadVersionException e) {
            logger.info("config znode got updated before save.");
        }

        // have to wait for an update.
        return false;
    }
}
