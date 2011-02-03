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
package io.s4.zeno.util;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

// TODO: Auto-generated Javadoc
/**
 * The Class ZooKeeperHelper.
 */
public class ZooKeeperHelper {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(ZooKeeperHelper.class);

    /** The zookeeper. */
    private ZooKeeper zookeeper = null;

    /** The n retries. */
    private int nRetries = 0;

    /** The retry time. */
    private int retryTime = 0;

    /**
     * Gets the zoo keeper.
     * 
     * @return the zoo keeper
     */
    public ZooKeeper getZooKeeper() {
        return zookeeper;
    }

    // We don't handle the case of reconnecting after session expiration.
    // This should be treated as a runtime exception.
    /**
     * Instantiates a new zoo keeper helper.
     * 
     * @param zookeeper
     *            the zookeeper
     * @param nRetries
     *            the n retries
     * @param retryTime
     *            the retry time
     */
    public ZooKeeperHelper(ZooKeeper zookeeper, int nRetries, int retryTime) {
        this.zookeeper = zookeeper;
        this.nRetries = nRetries;
        this.retryTime = retryTime;
    }

    /**
     * The Interface Operation.
     */
    private interface Operation {

        /**
         * Execute.
         * 
         * @return the object
         * @throws KeeperException
         *             the keeper exception
         * @throws InterruptedException
         *             the interrupted exception
         */
        public Object execute() throws KeeperException, InterruptedException;
    }

    /**
     * Attempt.
     * 
     * @param op
     *            the op
     * @return the object
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    private Object attempt(Operation op) throws KeeperException,
            InterruptedException {
        for (int i = 0; i < nRetries; ++i) {
            try {
                return op.execute();
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn("connection to zookeeper server lost. retrying.");
                retryWait(i);
            }
        }

        // last try!
        return op.execute();
    }

    /**
     * Exists.
     * 
     * @param path
     *            the path
     * @param watcher
     *            the watcher
     * @return the stat
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    public Stat exists(final String path, final Watcher watcher)
            throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.exists(path, watcher);
            }
        };

        return (Stat) attempt(op);
    }

    /**
     * Exists.
     * 
     * @param path
     *            the path
     * @param watch
     *            the watch
     * @return the stat
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    public Stat exists(final String path, final boolean watch)
            throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.exists(path, watch);
            }
        };

        return (Stat) attempt(op);
    }

    /**
     * Creates the.
     * 
     * @param path
     *            the path
     * @param data
     *            the data
     * @param acl
     *            the acl
     * @param createMode
     *            the create mode
     * @return the string
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    public String create(final String path, final byte[] data,
            final List<ACL> acl, final CreateMode createMode)
            throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.create(path, data, acl, createMode);
            }
        };

        return (String) attempt(op);
    }

    /**
     * Delete.
     * 
     * @param path
     *            the path
     * @param version
     *            the version
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    public void delete(final String path, final int version)
            throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                zookeeper.delete(path, version);
                return null;
            }
        };

        attempt(op);
    }

    /**
     * Sets the data.
     * 
     * @param path
     *            the path
     * @param data
     *            the data
     * @param version
     *            the version
     * @return the stat
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    public Stat setData(final String path, final byte[] data, final int version)
            throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.setData(path, data, version);
            }
        };

        return (Stat) attempt(op);
    }

    /**
     * Gets the data.
     * 
     * @param path
     *            the path
     * @param watcher
     *            the watcher
     * @param stat
     *            the stat
     * @return the data
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    public byte[] getData(final String path, final Watcher watcher,
            final Stat stat) throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.getData(path, watcher, stat);
            }
        };

        return (byte[]) attempt(op);
    }

    /**
     * Gets the data.
     * 
     * @param path
     *            the path
     * @param watch
     *            the watch
     * @param stat
     *            the stat
     * @return the data
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    public byte[] getData(final String path, final boolean watch,
            final Stat stat) throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.getData(path, watch, stat);
            }
        };

        return (byte[]) attempt(op);
    }

    /**
     * Gets the children.
     * 
     * @param path
     *            the path
     * @param watcher
     *            the watcher
     * @return the children
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    @SuppressWarnings("unchecked")
    public List<String> getChildren(final String path, final Watcher watcher)
            throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.getChildren(path, watcher);
            }
        };

        Object ret = attempt(op);
        return (ret != null) ? (List<String>) ret
                : Collections.<String> emptyList();
    }

    /**
     * Gets the children.
     * 
     * @param path
     *            the path
     * @param watch
     *            the watch
     * @return the children
     * @throws KeeperException
     *             the keeper exception
     * @throws InterruptedException
     *             the interrupted exception
     */
    @SuppressWarnings("unchecked")
    public List<String> getChildren(final String path, final boolean watch)
            throws KeeperException, InterruptedException {
        Operation op = new Operation() {
            public Object execute() throws KeeperException,
                    InterruptedException {
                return zookeeper.getChildren(path, watch);
            }
        };

        Object ret = attempt(op);
        return (ret != null) ? (List<String>) ret
                : Collections.<String> emptyList();
    }

    /**
     * Retry wait.
     * 
     * @param i
     *            the i
     * @throws InterruptedException
     *             the interrupted exception
     */
    private void retryWait(int i) throws InterruptedException {
        if (i > nRetries) return;

        Thread.sleep(retryTime);
    }
}
