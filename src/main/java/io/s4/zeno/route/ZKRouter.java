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
package io.s4.zeno.route;

import io.s4.zeno.Part;
import io.s4.zeno.config.ZKPaths;
import io.s4.zeno.util.ZenoError;
import io.s4.zeno.util.ZooKeeperHelper;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;


// TODO: Auto-generated Javadoc
/**
 * The Class ZKRouter.
 */
public class ZKRouter implements Router {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(ZKRouter.class);

    /** The zookeeper. */
    private ZooKeeperHelper zookeeper = null;

    /** The zkpath. */
    private ZKPaths zkpath = null;

    /** The route updater. */
    private RouteUpdater routeUpdater = null;

    /** The hold updater. */
    private HoldUpdater holdUpdater = null;

    /** The data socket. */
    private DatagramSocket dataSocket = null;

    /**
     * The Class Route.
     */
    private class Route {

        /** The hold. */
        private volatile boolean hold = false;

        /** The address. */
        private SocketAddress address = null;

        /** The queue. */
        private LinkedList<DatagramPacket> queue = null;

        /**
         * Instantiates a new route.
         */
        public Route() {
        }

        /**
         * Instantiates a new route.
         * 
         * @param host
         *            the host
         * @param port
         *            the port
         * @param hold
         *            the hold
         */
        @SuppressWarnings("unused")
        public Route(String host, int port, boolean hold) {
            address = new InetSocketAddress(host, port);
            hold = false;
        }

        /**
         * Send.
         * 
         * @param data
         *            the data
         * @return true, if successful
         */
        public boolean send(byte[] data) {
            byte[] packetData = Arrays.copyOf(data, data.length + 1);

            if (hold) {
                packetData[packetData.length - 1] = (byte) 1;
                DatagramPacket packet = new DatagramPacket(packetData,
                                                           packetData.length);

                LinkedList<DatagramPacket> q = queue;
                if (q == null) {
                    logger.warn("SEND failed: hold queue is null");
                    return false;
                }

                // We are either appending to the queue,
                // or dequeuing, never both. So no need to synchronize
                q.add(packet);

                logger.debug("packet added to queue");

            } else {
                DatagramPacket packet = new DatagramPacket(packetData,
                                                           packetData.length);

                SocketAddress a = address;
                if (a == null) {
                    logger.warn("SEND failed: destination address is null");
                    return false;
                }

                packet.setSocketAddress(a);

                try {
                    dataSocket.send(packet);
                    logger.debug("sent packet");

                } catch (Exception e) {
                    logger.error("SEND failed: " + e);
                    return false;
                }
            }

            return true;
        }

        /**
         * Sets the address.
         * 
         * @param dest
         *            the new address
         * @throws IOException
         *             Signals that an I/O exception has occurred.
         */
        public void setAddress(String dest) throws IOException {
            String[] hostport = dest.split(":");

            InetSocketAddress destSock = null;

            try {
                if (hostport.length == 2) {
                    destSock = new InetSocketAddress(hostport[0],
                                                     new Integer(hostport[1]));
                } else {
                    throw new IOException("Malformed host-port " + dest);
                }

            } catch (NumberFormatException e) {
                throw new IOException("Malformed host-port " + dest);
            }

            address = destSock;
        }

        /**
         * Unset address.
         */
        public void unsetAddress() {
            address = null;
        }

        /**
         * Sets the hold.
         * 
         * @return true, if successful
         */
        public boolean setHold() {
            if (hold) return false;

            if (queue == null) queue = new LinkedList<DatagramPacket>();

            hold = true;

            return true;
        }

        /**
         * Unset hold.
         * 
         * @return true, if successful
         */
        public boolean unsetHold() {
            if (!hold) return false;

            // unhold it right away. new events will not be queued
            hold = false;

            boolean error = false;

            LinkedList<DatagramPacket> q = queue;
            if (q != null) {
                DatagramPacket packet;

                // if route is put on hold mid-way, we will stop sending.
                while (!hold && !q.isEmpty()) {
                    packet = q.remove();
                    SocketAddress a = address;
                    if (a != null) {
                        try {
                            packet.setSocketAddress(a);
                            dataSocket.send(packet);

                        } catch (Exception e) {
                            logger.error("Error sending queued item: " + e);
                            error = true;
                        }
                    } else {
                        logger.warn("Purging packet: address is null");
                    }
                }
            }

            queue = null;

            return !error;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        public String toString() {
            return (address != null ? address.toString() : "NULL") + ':'
                    + (hold ? "hold" : "pass");
        }
    }

    /**
     * Instantiates a new zK router.
     * 
     * @param zookeeper
     *            the zookeeper
     * @param base
     *            the base
     */
    public ZKRouter(ZooKeeperHelper zookeeper, ZKPaths zkpath, Hasher hasher) {
        this.zookeeper = zookeeper;
        this.zkpath = zkpath;
        this.routeUpdater = new RouteUpdater();
        this.holdUpdater = new HoldUpdater();

        this.hasher = hasher;

        try {
            this.dataSocket = new DatagramSocket();
        } catch (SocketException e) {
            logger.error("error creating emission socket: " + e);
            this.dataSocket = null;
        }
    }

    /** The route map. */
    private ConcurrentHashMap<Part.Id, Route> routeMap = new ConcurrentHashMap<Part.Id, Route>();

    private final Hasher hasher;

    /**
     * Reverse route map.
     * 
     * @return the map
     */
    private Map<String, Set<Part.Id>> reverseRouteMap() {
        Map<String, Set<Part.Id>> reversed = new TreeMap<String, Set<Part.Id>>();

        for (Map.Entry<Part.Id, Route> r : routeMap.entrySet()) {
            String to = r.getValue().toString();

            Set<Part.Id> s;
            if (!reversed.containsKey(to))
                reversed.put(to, s = new TreeSet<Part.Id>());
            else
                s = reversed.get(to);

            s.add(r.getKey());
        }

        return reversed;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.router.Router#send(int, byte[])
     */
    public boolean send(int group, int key, byte[] data) {
        if (dataSocket != null) {
            Part.Id id = hasher.hash(group, key);

            if (id == null) return false;

            Route r = routeMap.get(id);

            if (r == null)
                return false;
            else
                return r.send(data);
        }

        return false;
    }

    // //////////////////////////////////////////////////////////
    // LOADING FROM ZK /////////////////////////////////////////
    // //////////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.router.Router#load()
     */
    public void load() {
        readMap();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return zkpath.zkBase + " autoreload:" + " routeMap:"
                + routeMap.toString() + "\nassignment:"
                + reverseRouteMap().toString();
    }

    /**
     * Read map.
     */
    private void readMap() {
        // scan partsBase
        logger.debug("reading router map from " + zkpath.partsBase);

        try {
            List<String> parts = zookeeper.getChildren(zkpath.partsBase
                    + "/items", new Watcher() {
                public void process(WatchedEvent w) {
                    readMap();
                }
            });

            ArrayList<Part.Id> partIds = new ArrayList<Part.Id>();

            // for each node, check route map and holds.
            // update routemap
            for (String p : parts) {
                Part.Id id = Part.Id.fromString(p);

                if (id != null) {
                    partIds.add(id);
                    updateDest(zkpath.routeMap(id.toString()));
                    updateHold(zkpath.routeHold(id.toString()));
                }
            }

            hasher.rebuild(partIds);

        } catch (KeeperException e) {
            logger.error("exception while listing parts: " + e);
            throw (new ZenoError("error reading routing map at "
                    + zkpath.partsBase, e));

        } catch (InterruptedException e) {
            logger.error("interrupted while listing parts");
        }

    }

    /**
     * Gets the id.
     * 
     * @param path
     *            the path
     * @return the id
     * @throws NumberFormatException
     *             the number format exception
     */
    private Part.Id getId(String path) throws NumberFormatException {
        return Part.Id.fromString(path.substring(path.lastIndexOf('/') + 1));
    }

    /**
     * Update hold.
     * 
     * @param path
     *            the path
     */
    private void updateHold(String path) {
        try {
            Part.Id id = getId(path);

            routeMap.putIfAbsent(id, new Route());

            if (zookeeper.exists(path, holdUpdater) != null) {
                routeMap.get(id).setHold();
            } else {
                routeMap.get(id).unsetHold();
            }

            logger.debug("updated hold for " + id + " -> " + routeMap.get(id)
                    + " (" + path + ")");

        } catch (KeeperException e) {
            logger.error("exception while updating routing hold from " + path
                    + " :" + e);
            throw (new ZenoError("error while updating routing hold state from "
                                         + path,
                                 e));

        } catch (InterruptedException e) {
            logger.error("interrupted while updating routing hold from " + path
                    + " :" + e);
        }

    }

    /**
     * Update dest.
     * 
     * @param path
     *            the path
     */
    private void updateDest(String path) {
        try {
            Part.Id id = getId(path);

            routeMap.putIfAbsent(id, new Route());
            Route r = routeMap.get(id);

            if (zookeeper.exists(path, routeUpdater) != null) {
                // get dest
                Stat stat = new Stat();
                String dest = new String(zookeeper.getData(path,
                                                           routeUpdater,
                                                           stat));

                // update map
                try {
                    r.setAddress(dest);
                } catch (IOException e) {
                    logger.error("address update failed for partid " + id
                            + ": " + e);
                }

            } else {
                // route deleted
                r.unsetAddress();
            }

            logger.debug("updated route for " + id + " -> " + routeMap.get(id)
                    + " (" + path + ")");

        } catch (KeeperException e) {
            throw (new ZenoError("error while updating routing destination from "
                                         + path,
                                 e));

        } catch (Exception e) {
            logger.error("exception while updating routing destination from "
                    + path + " :" + e);
        }
    }

    /**
     * The Class HoldUpdater.
     */
    public class HoldUpdater implements Watcher {

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent
         * )
         */
        public void process(WatchedEvent e) {
            String path = e.getPath();
            logger.info("updating routing hold from " + path);
            updateHold(path);
        }
    }

    /**
     * The Class RouteUpdater.
     */
    public class RouteUpdater implements Watcher {

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent
         * )
         */
        public void process(WatchedEvent e) {
            String path = e.getPath();
            logger.info("updating routing destination from " + path);
            updateDest(path);
        }
    }
}
