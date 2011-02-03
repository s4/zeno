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
package io.s4.zeno;

import java.util.concurrent.TimeoutException;

// TODO: Auto-generated Javadoc
/**
 * The Interface EventMonitor.
 */
public interface EventMonitor {

    public interface Factory {
        public EventMonitor getInstance();
    }

    /**
     * Put event.
     * 
     * @param length
     *            the length
     */
    void putEvent(double length);

    /**
     * Gets the millis since last event.
     * 
     * @return the millis since last event
     */
    long getMillisSinceLastEvent();

    /**
     * Wait for a period of silence. Blocks till a period of time passes without
     * any events. Times out after a specified period.
     * 
     * @param silentMs
     *            period of time for which silence should be observed
     *            (milliseconds).
     * 
     * @param timeoutMs
     *            timeout (milliseconds).
     * 
     * @throws TimeoutException
     *             if timeout expired.
     * @throws InterruptedException
     *             if interrupted while waiting.
     */
    void waitForSilence(long silentMs, long timeoutMs) throws TimeoutException, InterruptedException;

    /**
     * Gets the event rate.
     * 
     * @return the event rate
     */
    double getEventRate();

    /**
     * Gets the event length.
     * 
     * @return the event length
     */
    double getEventLength();

    /**
     * Checks if is valid.
     * 
     * @return true, if is valid
     */
    boolean isValid();

    /**
     * Reset.
     */
    void reset();
}
