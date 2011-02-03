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

import io.s4.zeno.config.ZKPaths;
import io.s4.zeno.util.ZooKeeperHelper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


// TODO: Auto-generated Javadoc
/**
 * The Class RouterTest.
 */
public class RouterTest {

    /** The router. */
    Router router;

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

        String connect = arg[0];
        int timeout = 1000;
        ZooKeeper zk = new ZooKeeper(connect, timeout, new Watcher() {
            public void process(WatchedEvent e) {
                System.out.println("Got ZK Notification: " + e);
            }
        });

        RouterTest test = new RouterTest(zk, arg[1]);

        test.dump();
        // test.test1();
        test.loadGen(arg[2]);
    }

    /**
     * Instantiates a new router test.
     * 
     * @param zk
     *            the zk
     * @param base
     *            the base
     */
    public RouterTest(ZooKeeper zk, String base) {
        router = new ZKRouter(new ZooKeeperHelper(zk, 3, 5000), new ZKPaths(base), new ModuloHasher());
        router.load();
    }

    /**
     * Dump.
     */
    public void dump() {
        System.out.println("ROUTES: " + router.toString());
    }

    /**
     * Test1.
     */
    public void test1() {
        byte[] data = (new String("1 5")).getBytes();

        router.send(0, 1, data);
    }

    /** The tset. */
    private HashSet<Thread> tset = new HashSet<Thread>();

    /**
     * Stop load.
     */
    public void stopLoad() {
        for (Thread t : tset)
            t.interrupt();

        tset = new HashSet<Thread>();
    }

    /**
     * Load gen.
     * 
     * @param filename
     *            the filename
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void loadGen(String filename) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(filename));

        stopLoad();

        String line = null;
        int n = 0;
        while ((line = in.readLine()) != null) {
            String[] parts = line.split("[ \t]+");
            ++n;

            if (parts.length < 3) {
                throw new IOException("incorrect number of fields on line " + n
                        + ": " + line);
            }

            try {
                int id = new Integer(parts[0]);
                int ts = (int) (1000.0 / (new Double(parts[1])));
                double tp = new Double(parts[2]);

                LoadGenerator lg = new LoadGenerator(0, id, ts, tp);
                Thread t;
                (t = new Thread(lg)).start();
                lg.t = t;
                tset.add(t);

            } catch (NumberFormatException e) {
                throw new IOException("malformed numbers on line " + n + ": "
                        + e);
            }
        }
    }

    /**
     * The Class LoadGenerator.
     */
    private class LoadGenerator implements Runnable {

        final int group;
        
        final int key;
        
        /** The ts. */
        final int ts;

        final String message;

        /** The t. */
        Thread t;

        /**
         * Instantiates a new load generator.
         * 
         * @param id
         *            the id
         * @param ts
         *            the ts
         * @param tp
         *            the tp
         */
        public LoadGenerator(int group, int key, int ts, double tp) {
            this.group = group;
            this.key = key;
            this.ts = ts;
            this.message = group + " " + key + " " + tp;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        public void run() {
            try {
                while (tset.contains(t)) {
                    Thread.sleep(ts);

                    if (!router.send(group, key, message.getBytes())) {
                        System.out.println("SEND failed for group=" + group + " key=" + key);
                    }
                }

            } catch (InterruptedException e) {
                System.out.println("interrupted. stopping load generation for group="
                        + group + " key=" + key);
            }
        }
    }
}
