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
package io.s4.zeno.protocol.helper;

import io.s4.zeno.Part;
import io.s4.zeno.Resource;
import io.s4.zeno.protocol.Command;
import io.s4.zeno.protocol.Connection;
import io.s4.zeno.resource.TimeSliceResource;

import java.io.IOException;

import org.apache.log4j.Logger;


// TODO: Auto-generated Javadoc
/**
 * The Class Sender.
 */
public class Sender {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(Sender.class);

    /** The conn. */
    private Connection conn;

    /**
     * Instantiates a new sender.
     * 
     * @param conn
     *            the conn
     */
    public Sender(Connection conn) {
        this.conn = conn;
    }

    public boolean hello(String name) {
        try {
            conn.out.println(name);
            String response = conn.in.readLine();

            logger.debug("handshake response: " + response);

            if (response.equals("OK")) return true;

        } catch (IOException e) {
            logger.error("error during handshake" + e);
        }

        return false;
    }

    /**
     * Goodbye.
     */
    public void goodbye() {
        conn.out.println(Command.Bye);
        conn.close();
    }

    /**
     * Gets the free resource.
     * 
     * @return the free resource
     */
    public Resource getFreeResource() {
        try {
            conn.out.println(Command.GetFree);

            Resource r = TimeSliceResource.fromString(conn.in.readLine());

            logger.debug("free resource: " + r);

            return r;

        } catch (IOException e) {
            logger.error("error while getting free resources from taker: " + e);
            return new TimeSliceResource(0);
        }
    }

    /**
     * Send part to a receiver.
     * 
     * @param part
     *            the part
     * @return true, if successful
     */
    public boolean sendPart(Part part) {
        try {
            // first pause the part. It may already be paused, but that's OK
            part.pause();

            // then send it over
            byte[] data = part.getData();

            logger.debug("sending part " + part.id());

            String command = Command.TakePart.toString() + '\n' + part.id()
                    + '\n' + data.length;
            conn.out.println(command);

            logger.debug(command);

            // conn.dataOut.write(data);
            // conn.dataOut.flush();

            String response = conn.in.readLine();
            logger.debug("got response: " + response);

            return (response != null && response.equals("OK"));

        } catch (IOException e) {
            logger.error("error while sending part data: " + e);
            return false;
        }
    }
}