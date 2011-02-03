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


public class DistributedSequenceTest implements Watcher {

    public static void main(String[] arg) throws IOException {
        PropertyConfigurator.configure("log4j.properties");

        // need an even number of args
        if (arg.length == 0 || arg.length % 2 == 1) {
            System.err.println("Usage: DistributedSequenceTest start1 length1 [start2 length2 ...]");

            System.exit(1);
        }

        List<Integer> start = new ArrayList<Integer>();
        List<Integer> length = new ArrayList<Integer>();
        int i = 0;

        for (String t : arg) {
            if (i % 2 == 0)
                start.add(new Integer(t));
            else
                length.add(new Integer(t));

            ++i;
        }

        DistributedSequenceTest test = new DistributedSequenceTest("localhost:2181",
                                                                   5000);

        int last = test.spawn(start, length);

        try {
            Thread.sleep(last);
        } catch (InterruptedException e) {
            System.err.println("Interrupted: " + e);
        }

        System.out.println("Should be done now");
    }

    DistributedSequence sequence = null;

    String connect = "";

    int timeout = 5000;

    public DistributedSequenceTest(String connect, int timeout) {
        this.connect = connect;
        this.timeout = timeout;
    }

    public void process(WatchedEvent e) {
        System.out.println("Received a notification: " + e);
    }

    private int spawn(List<Integer> start, List<Integer> length)
            throws IOException {
        if (sequence == null) {
            ZooKeeper zk = new ZooKeeper(connect, timeout, this);
            sequence = new DistributedSequence(new ZooKeeperHelper(zk, 3, 5000),
                                               "/tmp/dsequence");
        }

        int total = 0;
        int st = 0;

        for (int i = 0; i < start.size(); ++i) {
            st += start.get(i);
            TestRunner r = new TestRunner(i, st, length.get(i));
            (new Thread(r)).start();

            if (st > total)
                total = st + length.get(i);
            else
                total += length.get(i);
        }

        return total;
    }

    public static class Item implements DistributedSequence.Item {

        String message = "";

        public int length = 0;

        Item(String m, int t) {
            message = m;
            length = t;
        }

        public byte[] getSequenceData() {
            return message.getBytes();
        }

        public void doHeadAction() {
            System.out.println("Reached Head: [" + message + "]. Processing...");

            try {
                Thread.sleep(length);
            } catch (InterruptedException e) {
                System.out.println("Interrupted [" + message + "]");
            }

            System.out.println("Done [" + message + "]");
        }

        public String getMessage() {
            return message;
        }
    }

    private class TestRunner implements Runnable {

        private int id = 0;

        private int start = 0;

        private Item item = null;

        public TestRunner(int id, int start, int length) {
            this.id = id;
            this.start = start;
            item = new Item("Id:" + id, length);
        }

        public void run() {
            try {
                Thread.sleep(start);
            } catch (InterruptedException e) {
                System.out.println("Interrupted [" + item.getMessage() + "]");
            }

            System.out.println("Adding [" + item.getMessage() + "]");
            DistributedSequence.SequencedItem qi = sequence.add(item);

            if (qi == null)
                System.out.println("Returned null SequencedItem for "
                        + item.getMessage());

            try {
                while (!qi.isDone())
                    Thread.sleep(1000);

                System.out.println("Task " + id + ": state changed to DONE");
            } catch (InterruptedException e) {
                System.err.println("Interrupted.");
            }
        }
    }
}
