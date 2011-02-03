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
package io.s4.zeno.console;

import io.s4.zeno.config.ZKConfigMap;
import io.s4.zeno.config.ZKPaths;
import io.s4.zeno.route.RouterTest;
import io.s4.zeno.util.ZooKeeperHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


/**
 * A Console for monitoring a Zeno cluster.
 */
public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    // node configuration
    HashMap<String, ZKConfigMap> nodeConf = new HashMap<String, ZKConfigMap>();

    ZooKeeper zookeeper = null;

    RouterTest router = null;

    ZKPaths zkpath = null;

    public static void main(String[] argv) throws IOException, KeeperException {
        PropertyConfigurator.configure("log4j.properties");

        String zkserver = argv[0];
        String base = argv[1];

        logger.info("connecting to zookeeper: " + zkserver);

        int timeout = 1000;
        ZooKeeper zookeeper = new ZooKeeper(zkserver, timeout, new Watcher() {
            public void process(WatchedEvent e) {
                logger.info("Got Notification: " + e);
            }
        });

        logger.info("connected to zookeeper");

        Main cli = new Main(zookeeper, base);
        cli.run();
    }

    Main(ZooKeeper zookeeper, String base) {
        this.zookeeper = zookeeper;
        this.zkpath = new ZKPaths(base);
        this.router = new RouterTest(zookeeper, base);
    }

    /** The data socket. */
    DatagramSocket dataSocket = null;

    void run() throws IOException {

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        ZooKeeperHelper zookeeperHelper = new ZooKeeperHelper(zookeeper,
                                                              3,
                                                              5000);

        String line;

        try {
            this.dataSocket = new DatagramSocket();
        } catch (SocketException e) {
            logger.error("error creating emission socket: " + e);
            this.dataSocket = null;
            return;
        }

        while (true) {
            // command loop

            System.out.print("> ");

            if ((line = in.readLine()) == null) break;

            String[] parts = line.split(" +", 2);

            if (parts.length == 0) continue;

            String cmd = parts[0];

            if (cmd.equals("routes")) {
                // dump routing information
                router.dump();

            } else if (cmd.equals("exit")) {
                // The End.
                break;

            } else if (cmd.equals("loadgen")) {
                // start load generation using arg as filename.
                if (parts.length != 2) continue;

                router.loadGen(parts[1]);

            } else if (cmd.equals("loadstop")) {
                // stop load generation
                router.stopLoad();

            } else if (cmd.equals("info")) {
                // print status of a site.
                if (parts.length != 2) continue;

                String node = parts[1];

                ZKConfigMap conf = nodeConf.get(node);
                if (conf == null) {
                    nodeConf.put(node,
                                 (conf = new ZKConfigMap(zookeeperHelper,
                                                         zkpath.node(node))));
                }

                System.out.println("nodeinfo for " + node + ": "
                        + conf.toString());

            } else {
                
                // Send a message to a site.

                if (parts.length != 2) continue;

                String node = parts[0];
                byte[] data = parts[1].getBytes();

                ZKConfigMap conf = nodeConf.get(node);
                if (conf == null) {
                    nodeConf.put(node,
                                 (conf = new ZKConfigMap(zookeeperHelper,
                                                         zkpath.node(node))));
                }

                String host = conf.get("IPAddress");
                int port = conf.getInt("port.event", -1);

                if (host == null || port < 0) {
                    logger.error("missing host/port information for node "
                            + node + " at " + zkpath.node(node));
                    continue;
                }

                DatagramPacket packet = new DatagramPacket(data,
                                                           data.length,
                                                           new InetSocketAddress(host,
                                                                                 port));

                try {
                    logger.debug("sending command to " + node + " at " + host
                            + ":" + port);
                    dataSocket.send(packet);
                    logger.debug("sent");

                } catch (Exception e) {
                    logger.error("SEND failed: " + e);
                    continue;
                }

            }
        }

        router.stopLoad();
        dataSocket.close();
    }
}
