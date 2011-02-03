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

import io.s4.zeno.util.ZenoDefs;
import io.s4.zeno.util.ZenoError;
import io.s4.zeno.util.ZooKeeperHelper;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


/**
 * Distributed sequence of items.
 * 
 * Allows Items to be added to the tail of a sequence. When an item reaches the
 * head of the sequence, the doHeadAction() method is called. The item is
 * removed from the sequence when this callback returns.
 * 
 * The sequence is implemented as a directory in ZooKeeper with sequential
 * files.
 */
public class DistributedSequence {

    // Maintain a sequence of items.
    // An element is notified when it is at the head of the sequence.

    private final ZooKeeperHelper zookeeper;

    private final String dir;

    private final String prefix;

    private final String path;

    private static final char pSep = '/';

    private static final Logger logger = Logger.getLogger(DistributedSequence.class);

    /**
     * Instantiates a new distributed sequence.
     * 
     * @param zookeeper
     *            zookeeper
     * @param dir
     *            the directory within which the sequence exists.
     */
    public DistributedSequence(ZooKeeperHelper zookeeper, String dir) {
        this.zookeeper = zookeeper;
        this.dir = dir;
        this.prefix = "q-";
        this.path = this.dir + pSep + this.prefix;
    }

    /**
     * Instantiates a new distributed sequence.
     * 
     * @param zookeeper
     *            zookeeper
     * @param dir
     *            the directory within which the sequence exists.
     * @param prefix
     *            the prefix to use for queue znodes. Default is "q-"
     */
    public DistributedSequence(ZooKeeperHelper zookeeper, String dir,
            String prefix) {
        this.zookeeper = zookeeper;
        this.dir = dir;
        this.prefix = prefix;
        this.path = this.dir + this.prefix;
    }

    /**
     * Add an Item to the sequence.
     * 
     * @param item
     *            the item
     * @return the sequenced item
     */
    public DistributedSequence.SequencedItem add(DistributedSequence.Item item) {
        while (true) {
            try {
                // create a sequential znode.
                String fullName = zookeeper.create(path,
                                                   item.getSequenceData(),
                                                   ZenoDefs.zkACL,
                                                   CreateMode.EPHEMERAL_SEQUENTIAL);

                logger.debug("Created sequence znode: " + fullName);

                // extract the node name from the full name
                String nodeName = fullName.substring(fullName.lastIndexOf(pSep) + 1);

                Node node = new Node(nodeName, item); // create a new Node for
                                                      // this item
                (new Thread(node)).start(); // start a thread to let that node
                                            // complete the insertion process.

                return new DistributedSequence.SequencedItem(node);

            } catch (KeeperException.NoNodeException e) {
                logger.debug("Parent directory does not exist: " + dir);

                try {

                    zookeeper.create(dir,
                                     new byte[0],
                                     ZenoDefs.zkACL,
                                     CreateMode.PERSISTENT);
                    logger.info("Parent directory created: " + dir);
                    logger.debug("Retrying creating znode for item.");

                } catch (InterruptedException ee) {
                } catch (KeeperException.NodeExistsException ee) {
                    logger.debug("Someone else created parent directory");
                } catch (KeeperException ee) {
                    logger.error("Failed to create sequence directory: " + ee);
                    throw (new ZenoError("error creating sequence directory: "
                            + dir, ee));
                    // return null; // can't even create the parent dir!
                }

            } catch (InterruptedException e) {
            } catch (KeeperException e) {
                logger.error("Failed to create znode for item: " + e);
                throw (new ZenoError("error creating znode for sequened item in "
                                             + dir,
                                     e));
                // return null;
            }
        }
    }

    /**
     * An Item which can be added to the sequence.
     */
    public interface Item {

        /**
         * Gets the data that is written into the znode.
         * 
         * @return the sequence data
         */
        public byte[] getSequenceData();

        /**
         * Action to perform when this item reaches the head of the sequence.
         */
        public void doHeadAction();
    }

    /**
     * An item after it has been added to the sequence.
     */
    public static class SequencedItem implements Item {

        /** The node in the queue */
        private final Node node;

        /**
         * Instantiates a new sequenced item.
         * 
         * @param node
         *            the node
         */
        SequencedItem(Node node) {
            this.node = node;
        }

        public byte[] getSequenceData() {
            return node.getItem().getSequenceData();
        }

        public void doHeadAction() {
            node.getItem().doHeadAction();
        }

        /**
         * Removes the item from the queue.
         * 
         * @return true, if successful
         */
        public boolean remove() {
            return node.remove();
        }

        /**
         * Checks if item is active.
         * 
         * @return true, if is active
         */
        public boolean isActive() {
            return node.isActive();
        }

        /**
         * Checks if action is done.
         * 
         * @return true, if is done
         */
        public boolean isDone() {
            return node.isDone();
        }

        /**
         * Try to wait for the node to be done and return true if the action is
         * done. It is possible that the caller's thread may be interrupted
         * before the action is done. In this case, false is returned.
         * 
         * @return true if and only if the action has been completed.
         */
        public boolean awaitDone() {
            try {
                synchronized (node) {
                    node.wait();
                }
            } catch (InterruptedException e) {
                logger.info("interrupted while waiting for sequenced item to finish.");
            }

            return this.isDone();
        }
    }

    /**
     * A node in the sequence.
     * 
     * This object handles notifications when the predecessor node completes
     * processing.
     */
    private class Node implements Runnable, Watcher {

        /** The node name. */
        private final String nodeName;

        /** The item. */
        private final DistributedSequence.Item item;

        private volatile boolean active = false;

        private volatile boolean done = false;

        /**
         * Instantiates a new node.
         * 
         * @param nodeName
         *            the node name
         * @param item
         *            the item
         */
        public Node(String nodeName, DistributedSequence.Item item) {
            this.nodeName = nodeName;

            this.item = item;
        }

        /**
         * A node is active when it is watching its predecessor or is at
         * the head and performing its head action.
         * 
         * @return true, if is active
         */
        public boolean isActive() {
            return active;
        }

        /**
         * A node is done when it reaches the head and completes
         * performing its head action.
         * 
         * @return true, if is done
         */
        public boolean isDone() {
            return done;
        }

        /**
         * Gets the item.
         * 
         * @return the item
         */
        public Item getItem() {
            return item;
        }

        /**
         * Complete insertion of the node into the sequence.
         */
        public void run() {
            try {

                active = true;
                locate();

            } catch (KeeperException e) {
                throw (new ZenoError("error inserting node into distributed sequence: "
                                             + nodeName,
                                     e));

            } catch (Exception e) {
                logger.error(nodeName + ": " + e);
                e.printStackTrace();
            }
        }

        /**
         * Handle notification when the predecessor changes.
         * 
         * @param w
         *            the w
         */
        public void process(WatchedEvent w) {
            // Something changed.
            logger.debug(nodeName + ": notified with " + w);

            // If a node went away, relocate in the sequence.
            try {

                if (w.getType() == Watcher.Event.EventType.NodeDeleted)
                    locate();

            } catch (KeeperException e) {
                throw (new ZenoError("error processing notification for node="
                        + nodeName + " event=" + w, e));

            } catch (Exception e) {
                logger.error(nodeName + ": " + e);
                e.printStackTrace();
            }

            // Otherwise, do nothing.
        }

        /**
         * Remove a sequenced node.
         * 
         * @return true, if successful
         */
        public boolean remove() {
            if (!active) return true;

            try {
                active = false;

                logger.debug("Deleting " + dir + pSep + nodeName);

                zookeeper.delete((dir + pSep + nodeName), -1);

                logger.debug("Deleted " + dir + pSep + nodeName);

                return true;

            } catch (KeeperException e) {
                throw (new ZenoError("error deleting node from distributed sequence: "
                                             + nodeName,
                                     e));

            } catch (Exception e) {
                logger.error("Exception while deleting " + dir + pSep
                        + nodeName + ": " + e);

                return false;

            } finally {
                // let all threads waiting for this node know that it is not
                // going to run.
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }

        // SYNCHRONIZE?

        /**
         * Check the siblings of this node to to find location in
         * sequence. If at head, call item.doHeadAction()
         * 
         * @throws KeeperException
         *             the keeper exception
         * @throws InterruptedException
         *             the interrupted exception
         */
        private void locate() throws KeeperException, InterruptedException {
            if (active) {
                String predecessor = findAndWatchPredecessor(); // get my
                                                                // predecessor.

                if (predecessor == null) { // I am at the head of the sequence.

                    logger.debug("Reached head of sequence: " + nodeName);

                    item.doHeadAction();

                    logger.debug("Done processing: " + nodeName);

                    remove();

                    done = true;
                }
            }
        }

        /**
         * Find predecessor in sequence.
         * 
         * @return the string
         * @throws KeeperException
         *             the keeper exception
         * @throws InterruptedException
         *             the interrupted exception
         */
        private String findPredecessor() throws KeeperException,
                InterruptedException {
            // if the znode corresponding to this exists, find a predecessor.
            // null if this is the head, or this znode does not exist.

            List<String> siblings = zookeeper.getChildren(dir, false);

            String p = null;
            boolean foundSelf = false;

            logger.debug("Finding predecessor of " + nodeName + " [" + siblings
                    + "]");

            for (String s : siblings) {
                if (s.compareTo(nodeName) < 0
                        && (p == null || s.compareTo(p) > 0))
                    p = s;

                else if (s.equals(nodeName)) foundSelf = true;
            }

            logger.debug("FoundSelf:" + foundSelf + " p:" + p);

            return foundSelf ? p : null;
        }

        /**
         * Watch the predecessor for changes.
         * 
         * @return the string
         * @throws KeeperException
         *             the keeper exception
         * @throws InterruptedException
         *             the interrupted exception
         */
        private String findAndWatchPredecessor() throws KeeperException,
                InterruptedException {
            String predecessor = null;

            // make sure the predecessor exists and set a watch on it
            do {
                predecessor = findPredecessor();
            } while (predecessor != null
                    && zookeeper.exists(dir + pSep + predecessor, this) == null);

            logger.debug("Watching predecessor: " + predecessor);

            // at this point, predecessor is either null or exists and is being
            // watched.
            return predecessor;
        }
    }
}