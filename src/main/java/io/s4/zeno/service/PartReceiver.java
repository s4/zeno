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
package io.s4.zeno.service;

import io.s4.zeno.Job;
import io.s4.zeno.Part;
import io.s4.zeno.Service;
import io.s4.zeno.Site;
import io.s4.zeno.protocol.Command;
import io.s4.zeno.protocol.Connection;
import io.s4.zeno.protocol.ConnectionListener;
import io.s4.zeno.util.ActivityMonitor;

import java.io.IOException;

import org.apache.log4j.Logger;


/**
 * If some other site is trying to send work to this site, accept those parts
 * subject to availability here.
 */
public class PartReceiver extends Service {
    Logger logger = Logger.getLogger(PartReceiver.class);

    private final Site site;

    public PartReceiver(Site site) {
        super("part-receiver");
        this.site = site;
    }

    ConnectionListener listener = null;
    private volatile boolean run = false;

    @Override
    public void initialize() {
        createListener();
        setInitialDelay(5000); // 5 sec initial delay.
        setDelay(0); // no delay between actions
        run = true;
    }

    @Override
    public int share() {
        return 1;
    }

    @Override
    public void unblock() {
        run = false;
        if (listener != null) listener.close();
    }

    private void createListener() {
        if (listener != null) return;

        int pport = site.spec().getInt("port.receive.protocol", -1);
        int dport = site.spec().getInt("port.receive.data", -1);

        site.info().set("port.receive.protocol", String.valueOf(pport));
        site.info().set("port.receive.data", String.valueOf(dport));
        site.info().save();

        if (pport <= 0) {
            logger.error("missing or invalid setting for port.receive.protocol in site spec. expected positive int.");
            return;
        }

        if (dport <= 0) {
            logger.error("missing or invalid setting for port.receive.data in site spec. expected positive int.");
            return;
        }

        listener = ConnectionListener.createInstance(pport, dport);

        if (listener == null)
            logger.error("failed to create ConnectionListener for part receiver with dport="
                    + dport + ", pport=" + pport);

        logger.info("Listener created at dport=" + dport + " pport=" + pport);
    }

    // PROTOCOL
    //
    // SENDER RECEIVER
    // name ->
    // <- OK/FAILED
    // command ->
    // <- response
    // ...
    // bye ->
    //
    // DONE
    public void action() {
        if (!run) return;
        Connection conn = listener.accept();
        if (conn == null) return;

        if (!run) {
            conn.close();
            return;
        }

        logger.debug("connected to a sender.");

        try {
            String senderName = conn.in.readLine();

            // make sure some time has elapsed since last part_send
            ActivityMonitor sendActivity = site.registry()
                                               .getActivityMonitor("part_send");

            if (!sendActivity.isSilent(5000)) {
                logger.info("site is recovering from a recent send.");
                conn.out.println("FAILED recovering");
                return;
            }

            conn.out.println("OK");

            logger.info("started RECEIVE protocol with sender " + senderName);

            Receiver receiver = new Receiver(senderName, conn);

            if (!site.registry().tryLockAndRun("part_transfer", receiver)) {
                logger.info("site is busy with another part transfer. cannot receive any new parts now.");
                conn.out.println("FAILED busy");
                return;
            }

        } catch (IOException e) {
            logger.error("exception while reading name from sender.", e);

        } finally {
            conn.close();
        }
    }

    // Core logic for receiving parts.
    private class Receiver implements Runnable {
        public Receiver(String senderName, Connection conn) {
            this.senderName = senderName;
            this.conn = conn;
        }

        private Connection conn;
        private String senderName;

        public void run() {

            try {
                int partsTaken = 0;
                String line;

                cmd_loop: while ((line = conn.in.readLine()) != null) {
                    Command command = null;
                    try {
                        command = Command.valueOf(line);
                    } catch (IllegalArgumentException e) {
                        logger.error("illegal command from sender "
                                + senderName + ": " + line, e);

                        conn.out.println("FAILED illegal-command");
                        return;
                    }

                    switch (command) {
                        case Bye:
                            break cmd_loop;

                        case GetFree:
                            conn.out.println(site.loadMonitor()
                                                 .getFreeResource()
                                                 .toString());
                            break;

                        case TakePart:
                            if (acceptPart(conn, senderName)) partsTaken++;
                            break;

                        default:
                            logger.warn("unknown command: " + line);
                            conn.out.println("FAILED illegal-command");
                            break cmd_loop;
                    }
                }

                if (partsTaken > 0) {
                    logger.info("took over " + partsTaken
                            + " parts from sender " + senderName);

                    site.registry().addCount("part_recv", partsTaken);
                    site.registry().getActivityMonitor("part_recv").tick();

                } else {
                    logger.info("took over no parts from sender " + senderName);
                }

            } catch (IOException e) {
                logger.error("exception while talking to sender " + senderName,
                             e);
            }
        }
    }

    private boolean acceptPart(Connection conn, String senderName)
            throws IOException {
        try {
            // PARTID
            Part.Id id = Part.Id.fromString(conn.in.readLine());

            // DATA_LENGTH
            int len = new Integer(conn.in.readLine());

            // DATA
            byte[] data = new byte[len];

            logger.debug("request to take over partid " + id + " with "
                    + data.length + " bytes data from " + senderName);

            if (takeover(id, data)) {
                conn.out.println("OK");
                logger.debug("OK");
                return true;

            } else {
                conn.out.println("FAILED takeover");
                logger.debug("FAILED");
            }

        } catch (NumberFormatException e) {
            logger.error("malformed id or part length: " + e);
            conn.out.println("FAILED malformed-partid");
        }

        return false;
    }

    private boolean takeover(Part.Id id, byte[] data) {
        Job job = site.job();

        if (job == null) return false;

        Part part = job.takeoverPart(id);
        if (part == null) return false;

        // INJECT THE DATA

        part.start();

        return true;
    }

}
