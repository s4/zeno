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
package io.s4.zeno.coop;

// The holder "owns" a set of znodes.
// it can ACQUIRE and RELEASE nodes.
// Additionally, it can also TAKEOVER nodes

import io.s4.zeno.util.ZenoDefs;
import io.s4.zeno.util.ZenoError;
import io.s4.zeno.util.ZooKeeperHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;


/**
 * A set of non-blocking locks, with facilities for acquiring a set of locks.
 * <p>
 * Two primary methods are provided for acquiring locks:
 * {@link #acquire(String s)} tries to acquire the lock named {@code s},
 * {@link #acquire(int n)} tries to acquire a set of up to {@code n} locks.
 * <p>
 * If successfully acquired, a lock is guaranteed to be owned by exactly one
 * caller. Fairness is not guaranteed.
 * <p>
 * Example: Suppose there are three locks: LOCK1, LOCK2, LOCK3. Of them, LOCK1
 * has been acquired. There exist nodes representing ownership in the owners
 * directory. Every lock has a node in the owners directory. It has an integer
 * datum called the "ownerId". If a child node with name matching the ownerId
 * exists, then this signifies that the node has been acquired. Else it is free.
 * To acquire a lock, a client tries to create a child with name equal to the
 * ownerId. If this creation succeeds, then the lock is acquired. To take over a
 * node, the client (1) creates a node named ownerId+1, then (2) increments
 * ownerId, if 1 succeeds. To release a lock, the client deletes its ownership
 * marker. The locks are organized as follows in ZooKeeper:
 * 
 * <pre>
 *    baseDir/
 *        items/
 *            LOCK1
 *            LOCK2
 *            LOCK3
 *        owners/
 *            LOCK1/        [data=ownerId1]
 *                ownerId1  [data=owner message]
 *            LOCK2/        [date=ownerId2]
 *                (No Children)
 *            ...
 * </pre>
 */
public class NonblockingLockset {

    private static final Logger logger = Logger.getLogger(NonblockingLockset.class);

    private final ZooKeeperHelper zookeeper;

    private final String ownerDir;

    private final String lockDir;

    // private final String baseDir;

    public NonblockingLockset(ZooKeeperHelper zookeeper, String baseDir) {
        this.zookeeper = zookeeper;
        // this.baseDir = baseDir;
        this.ownerDir = baseDir + "/owners";
        this.lockDir = baseDir + "/items";
    }

    /**
     * Gets the name of the holder node for a named lock.
     * 
     * @param name
     *            the name of the lock
     * @return the holder node
     */
    private String getOwnerDir(String name) {
        return ownerDir + '/' + name;
    }

    /**
     * Test if a owner marker exists; if it is non-existent try to create it.
     * <p>
     * The holder directory of each lock is versioned. This method creates
     * 
     * @param path
     *            the path to the lock node's ownership node.
     * @param id
     *            revision id
     * @param idTest
     *            revision id that should be present. If it has changed, that
     *            means another caller created the marker already.
     * @param data
     *            data to be stored in the marker
     * @return true, if successful
     */
    private boolean testAndCreate(String path, int id, int idTest, byte[] data) {
        try {
            String znode = path + "/" + id;

            logger.debug("testAndCreate " + znode);

            // first test if path exists
            Stat stat = zookeeper.exists(znode, false);

            // yes => can't create
            if (stat != null) {
                logger.debug("znode already exists: " + znode);
                return false;
            }

            // no => try to create it.
            try {

                String name = zookeeper.create(znode,
                                               data,
                                               ZenoDefs.zkACL,
                                               CreateMode.EPHEMERAL);

                if (!name.isEmpty()) {
                    logger.debug("created: " + name);

                    // check if the id of path matched idTest
                    if (getOwnerId(path) == idTest)
                        return true;
                    else
                        zookeeper.delete(name, -1);
                }

                return false;

            } catch (KeeperException.NodeExistsException e) {
                logger.debug("znode already exists. looks like someone just created it: "
                        + path);
            } catch (KeeperException e) {
                logger.error("caught exception: " + e);
                throw (new ZenoError("error while creating id=" + id
                        + ", path=" + path, e));

            }
        } catch (KeeperException e) {
            logger.error("caught exception: " + e);
            throw (new ZenoError("error while testing existence of id=" + id
                    + ", path=" + path, e));

        } catch (InterruptedException e) {
            logger.error("interrupted: " + e);
        }

        return false;
    }

    // remove a path. messy exception handling taken care of here.
    private boolean remove(String path) {
        try {
            zookeeper.delete(path, -1);
            return true;
        } catch (KeeperException e) {
            logger.error("caught exception: " + e);
            throw (new ZenoError("error while removing path=" + path, e));

        } catch (InterruptedException e) {
            logger.error("interrupted: " + e);
        }

        return false;
    }

    // create a path. exception handling included.
    private boolean mkpath(String path) {
        try {
            if (zookeeper.exists(path, false) != null) return true;

            String name = zookeeper.create(path,
                                           ZenoDefs.emptyBytes,
                                           ZenoDefs.zkACL,
                                           CreateMode.PERSISTENT);

            if (!name.isEmpty()) return true;

            return false;

        } catch (KeeperException e) {
            logger.error("caught exception: " + e);
            throw (new ZenoError("error while creating path=" + path, e));

        } catch (InterruptedException e) {
            logger.error("interrupted: " + e);
        }

        return false;
    }

    // -1 => error
    private int getOwnerId(String path) {
        try {
            Stat stat = new Stat();
            byte[] data = zookeeper.getData(path, false, stat);

            if (data.length == 0) return 0;

            return Integer.parseInt(new String(data));

        } catch (KeeperException.NoNodeException e) {
            logger.debug(path + " doesn't exist. creating it");

            if (mkpath(path))
                return 0;
            else
                return -1;

        } catch (Exception e) {
            logger.error("caught exception: " + e);
            return -1;
        }
    }

    /**
     * Increment owner id.
     * 
     * @param path
     *            the path
     * @return true, if successful
     */
    private boolean incrementOwnerId(String path) {
        try {
            Stat stat = new Stat();
            byte[] data = zookeeper.getData(path, false, stat);

            int version = stat.getVersion();
            int id = 0;

            if (data.length > 0) id = Integer.parseInt(new String(data));

            ++id;

            zookeeper.setData(path, String.valueOf(id).getBytes(), version);

            return true;

        } catch (Exception e) {
            logger.error("caught exception: " + e);
        }

        return false;
    }

    /**
     * create a new owner path.
     * 
     * @param name
     * @param data
     * @return
     */
    public boolean createNewLock(String name, byte[] data) {
        String ownerPath = getOwnerDir(name);
        String lockPath = lockDir + '/' + name;

        try {
            if ((zookeeper.exists(lockPath, false) == null)
                    && (zookeeper.exists(ownerPath, false) == null)) {

                // create holder for owner marker
                zookeeper.create(ownerPath, "0".getBytes(), // version is 0
                                                            // initially
                                 ZenoDefs.zkACL,
                                 CreateMode.PERSISTENT);

                // create marker
                String znode = ownerPath + "/0";
                zookeeper.create(znode,
                                 data,
                                 ZenoDefs.zkACL,
                                 CreateMode.EPHEMERAL);

                // then create a lock node
                zookeeper.create(lockPath,
                                 ZenoDefs.emptyBytes,
                                 ZenoDefs.zkACL,
                                 CreateMode.PERSISTENT);

                return true;
            }
        } catch (KeeperException.NodeExistsException e) {
            logger.error("Error creating new owner node.", e);
        } catch (Exception e) {
            logger.error("Error creating new owner node.", e);
        }

        return false;
    }

    /**
     * Acquire a ZNode. It is guaranteed that exactly one caller returns
     * successfully. Some data is written to the node if it is acquired
     * successfully.
     * 
     * @param name
     *            name of znode to acquire.
     * @param data
     *            data to be written to znode if acquired successfully.
     * @return true, if successful
     */
    public boolean acquire(String name, byte[] data) {
        logger.debug("attempting to acquire " + name);

        LockStatus status = lockSet.status(name);

        if (status != null) {
            String holder = getOwnerDir(name);

            // int id = getOwnerId(holder);

            int id = status.version;

            if (id < 0) return false; // this node cannot be acquired

            mkpath(holder);

            return testAndCreate(holder, id, id, data);
        } else {
            logger.error("unknown lock " + name);
            return false;
        }
    }

    /**
     * Acquire a ZNode. It is guaranteed that exactly one caller returns
     * successfully. A string is written to the node if it is acquired
     * successfully.
     * 
     * @param name
     *            name of znode to acquire.
     * @param message
     *            data to write in the znode, if acquired.
     * @return true, if successful
     */
    public boolean acquire(String name, String message) {
        return acquire(name, message.getBytes());
    }

    /**
     * Acquire a ZNode. It is guaranteed that exactly one caller returns
     * successfully.
     * 
     * @param name
     *            name of the znode to acquire.
     * @return true, if successful
     */
    public boolean acquire(String name) {
        return acquire(name, ZenoDefs.emptyBytes);
    }

    /**
     * Release a versioned lock.
     * 
     * @param name
     *            the name
     * @param id
     *            the version of the lock.
     * @return true, if successful
     */
    public boolean release(String name, int id) {
        String holder = getOwnerDir(name);

        if (id < 0) return false; // this node cannot be released

        String path = holder + '/' + id;

        return remove(path);
    }

    /**
     * Release a lock, independent of its version.
     * 
     * @param name
     *            the name
     * @return true, if successful
     */
    public boolean release(String name) {
        String holder = getOwnerDir(name);

        int id = getOwnerId(holder);

        if (id < 0) return false; // this node cannot be released

        String path = holder + '/' + id;

        return remove(path);
    }

    /**
     * Gets the current version of a lock.
     * 
     * @param name
     *            the name
     * @return the version
     */
    public int getVersion(String name) {
        return getOwnerId(getOwnerDir(name));
    }

    /**
     * Takeover a lock. Assumption: only one client is attempting takeover.
     * 
     * @param name
     *            lock name
     * @param data
     *            data to write into znode
     * @return true, if successful
     */
    public boolean takeover(String name, byte[] data) {
        logger.debug("taking over " + name);
        LockStatus status = lockSet.status(name);

        if (status != null) {
            String holder = getOwnerDir(name);

            // int id = getOwnerId(holder);
            int id = status.version;

            logger.debug("id[" + name + "]: " + id);

            if (id < 0) return false; // this node cannot be taken over

            mkpath(holder);

            return (testAndCreate(holder, id + 1, id, data) && incrementOwnerId(holder)); // IMPORTANT
        } else {
            logger.error("unknown lock " + name);
            return false;
        }
    }

    /**
     * Takeover a lock. Assumption: only one client is attempting takeover.
     * 
     * @param name
     *            lock name
     * @return true, if successful
     */
    public boolean takeover(String name) {
        return takeover(name, ZenoDefs.emptyBytes);
    }

    /**
     * Acquire.
     * 
     * @param n
     *            the n
     * @param data
     *            the data
     * @return the sets the
     */
    public List<String> acquire(int n, byte[] data) {
        List<String> nodes = new ArrayList<String>();

        if (n > 0) {
            // List<String> allNodes = getValidNames();
            List<String> freeNodes = lockSet.free();
            Collections.shuffle(freeNodes); // this reduces herd effect.
            logger.debug("free nodes: " + freeNodes);

            for (String c : freeNodes) {
                if (acquire(c, data)) {
                    nodes.add(c);
                    --n;
                    if (n == 0) break;
                }
            }
        }

        return nodes;
    }

    public List<String> acquire(int n, String message) {
        return acquire(n, message.getBytes());
    }

    /**
     * Acquire.
     * 
     * @param n
     *            the n
     * @return the sets the
     */
    public List<String> acquire(int n) {
        return acquire(n, ZenoDefs.emptyBytes);
    }

    /**
     * Acquire.
     * 
     * @param data
     *            the data
     * @return the string
     */
    public String acquireOne(byte[] data) {
        String node = null;

        for (String c : getValidNames()) {
            if (acquire(c, data)) {
                node = c;
                break;
            }
        }

        return node;
    }

    public String acquireOne(String message) {
        return acquireOne(message.getBytes());
    }

    /**
     * Acquire.
     * 
     * @return the string
     */
    public String acquireOne() {
        return acquireOne(ZenoDefs.emptyBytes);
    }

    public void awaitUpdate() {
        lockSet.awaitUpdate();
    }

    /**
     * Gets the valid names.
     * 
     * @return the valid names
     */
    private List<String> getValidNames() {
        return new ArrayList<String>(lockSet.all());
    }

    private enum NodeType {
        lockDir,
        ownerDir,
        lock,
        owner,
        marker,
        unknown
    };

    private static class LockStatus {
        public int version;
        public boolean free;

        public LockStatus(int version, boolean free) {
            this.version = version;
            this.free = free;
        }
    }

    class LockSet implements Watcher {
        private ConcurrentHashMap<String, LockStatus> locks = null;

        public LockStatus status(String name) {
            if (locks == null) loadAll();

            return locks.get(name);
        }

        public List<String> all() {
            if (locks == null) loadAll();

            return new ArrayList<String>(locks.keySet());
        }

        public List<String> free() {
            if (locks == null) loadAll();

            ArrayList<String> free = new ArrayList<String>();
            for (Map.Entry<String, LockStatus> e : locks.entrySet()) {
                if (e.getValue().free) free.add(e.getKey());
            }

            return free;
        }

        private void loadAll() {
            logger.debug("loading all lock names from " + lockDir);

            locks = new ConcurrentHashMap<String, LockStatus>();

            try {
                List<String> children = zookeeper.getChildren(lockDir, this);

                for (String l : children) {
                    loadLock(l);
                }

            } catch (KeeperException e) {
                logger.error("caught exception: " + e);
                throw (new ZenoError("error getting children of " + lockDir, e));

            } catch (InterruptedException e) {
                logger.info("interrupted while getting children of " + lockDir,
                            e);
            }
        }

        private int getId(String l) {
            String path = getOwnerDir(l);
            try {
                Stat stat = new Stat();
                byte[] data = zookeeper.getData(path, this, stat);

                if (data.length == 0) return 0;

                return Integer.parseInt(new String(data));

            } catch (KeeperException.NoNodeException e) {
                return 0;

            } catch (Exception e) {
                logger.error("caught exception: " + e);
                return -1;
            }
        }

        private void loadLock(String l) {
            try {
                int id = getId(l);
                LockStatus lockStatus;
                if (id < 0) {
                    lockStatus = new LockStatus(0, false);
                } else {
                    String path = getOwnerDir(l) + '/' + id;
                    Stat stat = zookeeper.exists(path, this);
                    boolean isFree = (stat == null);

                    lockStatus = new LockStatus(id, isFree);
                }

                locks.put(l, lockStatus);

            } catch (Exception e) {
                logger.error("error while loading lock " + l, e);
            }
        }

        private volatile CountDownLatch _u = new CountDownLatch(1);

        private void update() {
            _u.countDown();
            _u = new CountDownLatch(1);
        }

        public void awaitUpdate() {
            try {
                _u.await();
            } catch (InterruptedException e) {
                logger.info("interupted while awaiting update", e);
            }
        }

        public void process(WatchedEvent e) {
            logger.info("Lock set received notification: " + e);

            String path = e.getPath();
            if (path == null) return;

            NodeType nodeType = detectNodeType(path);
            switch (nodeType) {
                case lockDir:
                    switch (e.getType()) {
                        case NodeChildrenChanged:
                            // children have changed. reload...
                            loadAll();
                            update();
                            break;
                        default:
                            logger.info("nothing to do for this notification.");
                    }
                    break;

                case lock:
                    // nothing to do here. locks contain no data.
                    // for lock creation/deletion, a watch on the lockDir would
                    // have also triggered.
                    break;

                case ownerDir:
                    // no data in ownerDir
                    // nothing to do if children are added or deleted.
                    // individual children are being watched. those watches will
                    // do the work.
                    break;

                case owner:
                case marker:
                    loadLock(getLockName(path, nodeType));
                    update();

            }
        }

        private NodeType detectNodeType(String path) {
            if (path.equals(lockDir)) return NodeType.lockDir;
            if (path.equals(ownerDir)) return NodeType.ownerDir;
            if (path.startsWith(lockDir)) return NodeType.lock;
            if (path.startsWith(ownerDir)) {
                // if there is one more slash after ownerDir, path is a marker;
                // else
                // it is an owner path
                int lastSlashIdx = path.lastIndexOf('/');
                return (lastSlashIdx > ownerDir.length()) ? NodeType.marker
                        : NodeType.owner;
            }

            return NodeType.unknown;
        }

        private String getLockName(String path, NodeType type) {
            switch (type) {
                case lock:
                    return path.substring(lockDir.length() + 1);

                case owner:
                    return path.substring(ownerDir.length() + 1);

                case marker:
                    return path.substring(ownerDir.length() + 1,
                                          path.lastIndexOf('/'));

                default:
                    return null;
            }
        }
    }

    private LockSet lockSet = new LockSet();
}
