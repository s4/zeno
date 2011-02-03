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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

public class ConnectionListener {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(ConnectionListener.class);

    /** The psock server. */
    private ServerSocket psockServer = null;

    /** The dsock server. */
    private ServerSocket dsockServer = null;


    public ConnectionListener(ServerSocket psockServer, ServerSocket dsockServer) {
        this.psockServer = psockServer;
        this.dsockServer = dsockServer;
    }

    public static ConnectionListener createInstance(int pport, int dport) {
        logger.debug("creating protocol and data ports. proto=" + pport
                + " data=" + dport);

        ServerSocket psockServer;
        ServerSocket dsockServer;

        try {
            psockServer = new ServerSocket(pport);
        } catch (IOException e) {
            logger.error("could not create server socket on pport " + pport
                    + ": " + e);
            return null;
        }

        try {
            dsockServer = new ServerSocket(dport);
        } catch (IOException e) {
            logger.error("could not create server socket on dport " + dport
                    + ": " + e);
            try {
                psockServer.close();
            } catch (IOException e1) {
                logger.error("error closing server socket at pport " + pport
                        + " after failing to create server socket at dport "
                        + dport);
            }

            return null;
        }

        return new ConnectionListener(psockServer, dsockServer);
    }

    /**
     * Good.
     * 
     * @return true, if successful
     */
    public boolean good() {
        return psockServer != null && dsockServer != null;
    }

    /**
     * Accept.
     * 
     * @return the connection
     */
    public Connection accept() {
        try {
            Socket psock = psockServer.accept();
            Socket dsock = dsockServer.accept();

            return new Connection(psock, dsock);

        } catch (IOException e) {
            logger.error("could not accept connections: " + e);
            close();
            return null;
        }
    }

    /**
     * Close.
     */
    public void close() {
        try {
            if (psockServer != null) psockServer.close();
            psockServer = null;

        } catch (IOException e) {
            logger.error("could not close protocol socket connection: " + e);
        }

        try {
            if (dsockServer != null) dsockServer.close();
            dsockServer = null;

        } catch (IOException e) {
            logger.error("could not close data socket connection: " + e);
        }
    }
}