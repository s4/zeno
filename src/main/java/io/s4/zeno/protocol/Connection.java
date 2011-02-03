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
package io.s4.zeno.protocol;

import io.s4.zeno.config.ConfigMap;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;


// TODO: Auto-generated Javadoc
/**
 * The Class Connection.
 */
public class Connection {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(Connection.class);

    /** The psock. */
    private Socket psock = null;

    /** The dsock. */
    private Socket dsock = null;

    /** The out. */
    public PrintWriter out = null;

    /** The in. */
    public BufferedReader in = null;

    /** The data out. */
    public BufferedOutputStream dataOut = null;

    /** The data in. */
    public BufferedInputStream dataIn = null;

    /**
     * Instantiates a new connection.
     * 
     * @param psock
     *            the psock
     * @param dsock
     *            the dsock
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public Connection(Socket psock, Socket dsock) throws IOException {
        this.psock = psock;
        this.dsock = dsock;

        out = new PrintWriter(psock.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(psock.getInputStream()));

        dataOut = new BufferedOutputStream(dsock.getOutputStream());
        dataIn = new BufferedInputStream(dsock.getInputStream());
    }

    /**
     * Close.
     */
    public void close() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (psock != null) psock.close();

            in = null;
            out = null;
            psock = null;

        } catch (IOException e) {
            logger.error("could not close protocol socket connection", e);
        }

        try {
            if (dataIn != null) dataIn.close();
            if (dataOut != null) dataOut.close();
            if (dsock != null) dsock.close();

            dataIn = null;
            dataOut = null;
            dsock = null;

        } catch (IOException e) {
            logger.error("could not close data socket connection", e);
        }
    }

    /**
     * Creates the to.
     * 
     * @param taker
     *            the taker
     * @return the connection
     */
    public static Connection createTo(ConfigMap taker) {
        String name = taker.get("name");
        String host = taker.get("IPAddress");
        int pport = taker.getInt("port.receive.protocol", -1);
        int dport = taker.getInt("port.receive.data", -1);

        if (host == null || pport < 0 || dport < 0) {
            logger.error("cannot create connection to taker " + name
                    + ". insufficient information");
            return null;
        }

        Socket psock = null;
        Socket dsock = null;
        Connection conn = null;

        try {
            logger.debug("protocol sock to " + name + " at " + host + ":"
                    + pport);
            psock = new Socket(host, pport);

            logger.debug("data sock to " + name + " at " + host + ":" + dport);
            dsock = new Socket(host, dport);

            conn = new Connection(psock, dsock);
            return conn;

        } catch (IOException e0) {
            logger.error("could not connect to taker " + name, e0);
            try {
                if (psock != null) psock.close();
            } catch (IOException e) {
            }
            try {
                if (dsock != null) dsock.close();
            } catch (IOException e) {
            }

            return null;
        }
    }

}