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

import io.s4.zeno.util.ZooKeeperHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


// TODO: Auto-generated Javadoc
/**
 * The Class ZKConfigTest.
 */
public class ZKConfigTest {

    /**
     * The main method.
     * 
     * @param arg
     *            the arguments
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void main(String[] arg) throws IOException {
        PropertyConfigurator.configure("log4j.properties");

        String connect = "localhost:2181";
        int timeout = 1000;
        ZooKeeper zk = new ZooKeeper(connect, timeout, new Watcher() {
            public void process(WatchedEvent e) {
                System.out.println("Got Notification: " + e);
            }
        });

        ZooKeeperHelper zookeeper = new ZooKeeperHelper(zk, 3, 5000);

        ZKWritableConfigMap conf = new ZKWritableConfigMap(zookeeper,
                                                           "/tmp/test-config");

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        String cmd;
        while ((cmd = in.readLine()) != null) {
            if (cmd.equals("get")) {
                String key = in.readLine();
                System.out.println("got '" + key + "': " + conf.get(key));
            } else {
                String key = in.readLine();
                String val = in.readLine();
                conf.set(key, val);
            }
        }
    }
}
