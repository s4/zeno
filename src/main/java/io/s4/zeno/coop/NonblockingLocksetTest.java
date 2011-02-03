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

import io.s4.zeno.util.ZooKeeperHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


// TODO: Auto-generated Javadoc
/**
 * The Class ZNodeHolderTest.
 */
public class NonblockingLocksetTest implements Watcher {

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

        // need an even number of args
        if (arg.length == 0) {
            System.err.println("Usage: ZNodeHolderTest size1 [size2 size3 ...] [X] [takeover1 takeover2 ...]");

            System.exit(1);
        }

        List<Integer> size = new ArrayList<Integer>();
        List<String> take = new ArrayList<String>();

        boolean sizes = true;

        for (String t : arg) {
            if (t.equals("X")) {
                sizes = false;
                continue;
            }

            if (sizes)
                size.add(new Integer(t));
            else
                take.add(t);
        }

        NonblockingLocksetTest test = new NonblockingLocksetTest("localhost:2181", 5000);

        test.spawn(size, take);
    }

    /** The connect. */
    String connect = "";

    /** The timeout. */
    int timeout = 5000;

    /**
     * Instantiates a new z node holder test.
     * 
     * @param connect
     *            the connect
     * @param timeout
     *            the timeout
     */
    public NonblockingLocksetTest(String connect, int timeout) {
        this.connect = connect;
        this.timeout = timeout;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    public void process(WatchedEvent e) {
        System.out.println("Received a notification: " + e);
    }

    /** The zk. */
    private ZooKeeper zk = null;

    /**
     * Spawn.
     * 
     * @param size
     *            the size
     * @param take
     *            the take
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void spawn(List<Integer> size, List<String> take)
            throws IOException {
        if (zk == null) zk = new ZooKeeper(connect, timeout, this);

        ZooKeeperHelper zookeeper = new ZooKeeperHelper(zk, 3, 5000);

        for (Integer s : size) {
            TestRunner r = new TestRunner(s, zookeeper);
            (new Thread(r)).start();
        }

        System.out.println("Sleeping a second...");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            System.out.println("interrupted: " + e);
        }

        NonblockingLockset holder = new NonblockingLockset(zookeeper, "/tmp/grab");
        for (String t : take) {
            if (holder.takeover(t))
                System.out.println("Taken over: " + t);
            else
                System.out.println("Takeover failed: " + t);
        }
    }

    /**
     * The Class TestRunner.
     */
    private class TestRunner implements Runnable {

        /** The s. */
        private int s = 0;

        /** The holder. */
        NonblockingLockset holder = null;

        /**
         * Instantiates a new test runner.
         * 
         * @param s
         *            the s
         * @param zk
         *            the zk
         */
        public TestRunner(int s, ZooKeeperHelper zk) {
            this.s = s;
            holder = new NonblockingLockset(zk, "/tmp/grab");
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        public void run() {
            System.out.println("Grabbing " + s + " nodes.");
            List<String> names = holder.acquire(s);

            if (names == null) {
                System.out.println("Got null names set");
                return;
            }

            System.out.println("Wanted " + s + ": got " + names);
        }
    }
}
