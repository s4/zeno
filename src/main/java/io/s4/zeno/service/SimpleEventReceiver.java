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

import io.s4.zeno.Part;
import io.s4.zeno.Service;
import io.s4.zeno.Site;
import io.s4.zeno.route.Hasher;
import io.s4.zeno.util.ZenoError;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.apache.log4j.Logger;


/**
 * Receive events and update load monitor.
 */
public class SimpleEventReceiver extends Service {
    private static final Logger logger = Logger.getLogger(SimpleEventReceiver.class);

    public SimpleEventReceiver(Site site, Hasher hasher) {
        super("simple-event-receiver");
        this.site = site;
        this.hasher = hasher;
    }

    private final Site site;

    private final Hasher hasher;

    private DatagramSocket dsock;

    protected void initialize() {
        int p = site.spec().getInt("port.event", -1);
        if (p <= 0) {
            throw new ZenoError("mising or invalid property port.event in site spec.");
        }

        site.info().set("port.event", String.valueOf(p));
        site.info().save();

        try {
            dsock = new DatagramSocket(p);
            logger.info("initialized with datagram sock: " + dsock.toString());
        } catch (IOException e) {
            logger.error("exception while creating datagram sock", e);
            dsock = null;
        }

        setInitialDelay(5000);
        setDelay(1000);
    }

    @Override
    public int share() {
        return 1;
    }

    @Override
    protected void action() {
        if (dsock == null) return;

        while (dsock.isBound()) {
            byte[] data = new byte[1024];
            DatagramPacket packet = new DatagramPacket(data, 1024);

            try {
                dsock.receive(packet);
            } catch (Exception e) {
                System.out.println("error: " + e);
                continue;
            }

            String command = new String(packet.getData(), 0, packet.getLength());
            // System.out.println("packet data: '" + command + "'");

            if (command.equals("pause") || command.equals("pause\n")) {
                site.job().pause();
                logger.info("STATE: " + site.state());
                continue;
            } else if (command.equals("resume") || command.equals("resume\n")) {
                site.job().unpause();
                logger.info("STATE: " + site.state());
                continue;
            }

            String line = new String(packet.getData(),
                                     0,
                                     packet.getLength() - 1);

            boolean isQueued = (packet.getData()[packet.getLength() - 1] == (byte) 1);

            String[] parts = line.split(" ");
            if (parts.length < 3) continue;

            try {
                int group = Integer.parseInt(parts[0]);
                int key = Integer.parseInt(parts[1]);
                Part.Id id = hasher.hash(group, key);

                double t = Double.parseDouble(parts[2]);

                if (id == null) {
                    logger.warn("malformed event identifiers: " + parts[0]
                            + "," + parts[1]);
                    continue;
                }

                Part part = site.job().partMap().get(id);

                // make sure this event belongs to a part in this site.
                if (part == null) {
                    logger.error("received an event for a part that is not owned by this site. partid: "
                            + id);
                    continue;
                }

                // Do not count events that are played from queue for the
                // purposes of monitoring
                if (!isQueued) {
                    logger.debug("GOT event: " + id);
                    site.eventMonitor().putEvent(t);
                    part.eventMonitor().putEvent(t);

                    // System.out.println("rate:"
                    // + site.eventMonitor().getEventRate()
                    // + " length:"
                    // + site.eventMonitor().getEventLength());
                    // System.out.println("load:"
                    // + site.loadMonitor().getLevel());
                    // System.out.println("free:" +
                    // site.loadMonitor().getFreeResource());

                } else {
                    logger.info("RECEIVED QUEUED EVENT FOR PARTID: " + id);
                }

            } catch (NumberFormatException e) {
                logger.error("malformed numbers in data: " + e);
            }

        }
    }
}
