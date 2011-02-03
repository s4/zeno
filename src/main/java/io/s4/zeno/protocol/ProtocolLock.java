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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * Re-entrant lock to manage concurrency in protocol.
 */
public class ProtocolLock {

    private static final Logger logger = Logger.getLogger(ProtocolLock.class);
    
    private ReentrantLock lock = new ReentrantLock(true);

    /**
     * Blocks till lock is obtained.
     * 
     * @see #tryTransferLock()
     * 
     * @return the transfer lock
     */
    public void get() {
        lock.lock();
    }

    /**
     * Release transfer lock. This signifies the end of an operation and allows
     * other operations to run.
     */
    public void release() {
        lock.unlock();
    }

    /**
     * Try to obtain transfer lock within a certain amount of time.
     * 
     * @param t
     *            time in milliseconds within which lock must be acquired.
     * @return true, if successful. If thread is interrupted or lock could not
     *         be acquired within t ms, returns false.
     */
    public boolean tryGet(int t) {
        try {
            return lock.tryLock(t, TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {
            logger.debug("interrupted while waiting for lock", e);
            return false;
        }
    }

    /**
     * Try to obtain transfer lock. Returns immediately with status of obtaining
     * lock. Note that {@ref #getTransferLock()} blocks.
     * 
     * @return true, if lock was obtained. Otherwise false.
     */
    public boolean tryGet() {
        return tryGet(0);
    }

    public String toString() {
        return lock.toString();
    }
}