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
package io.s4.zeno.util;

import java.util.concurrent.TimeoutException;

/**
 * Utility for letting a period of time elapse between actions.
 */
public class ActivityMonitor {

    /** The last time at which an action occurred. */
    private long lastTime = 0;

    /**
     * Notify object that an action has occurred.
     */
    public void tick() {
        lastTime = System.currentTimeMillis();
    }

    /**
     * Has enough time lapsed since last action (i.e. last call to
     * {@link #tic()})?
     * 
     * @return true if no action has occurred or if more than lapse milliseconds
     *         have occurred since last action.
     */
    public boolean isSilent(long s) {
        long lt = lastTime;

        return (lt == 0) || (System.currentTimeMillis() >= lt + s);
    }

    private long remainingForSilence(long s) {
        return Math.max(0, (s - timeSinceLast()));
    }

    /**
     * Blocks till at least {@code t} ms have elapsed since the last action.
     * <p>
     * At that time {@link #isReady()} returns true. If the object is ready when
     * this function is called, it returns immediately. The granularity of
     * blocking time depends on the accuracy provided by the system clock. See
     * {@link Thread#sleep(long)}.
     * 
     * <p>
     * In case other threads notify this object of actions by calling
     * {@link #tick()}, the function blocks till atleast {@code t} ms have
     * elapsed since <i>all</i> those actions.
     * 
     * @throws InterruptedException
     *             if the thread is interrupted while sleeping.
     * 
     */
    public void waitForSilence(long s) throws InterruptedException {
        while (!isSilent(s)) {
            Thread.sleep(remainingForSilence(s));
        }
    }

    /**
     * Blocks till at least {@code lapse} ms have elapsed since the last action,
     * or a timeout has expired. If the timeout has expired, an exception is
     * thrown.
     * <p>
     * Upon successfully returning, {@link #isReady()} returns true. If the
     * object is ready when this function is called, it returns immediately. The
     * granularity of blocking time depends on the accuracy provided by the
     * system clock. See {@link Thread#sleep(long)}.
     * 
     * <p>
     * In case other threads notify this object of actions by calling
     * {@link #tic()}, the function blocks till {@code lapse} ms have elapsed
     * since <i>all</i> those actions.
     * 
     * @param timeoutMs
     *            timeout in milliseconds.
     * @throws InterruptedException
     *             if the thread is interrupted while sleeping.
     * @throws TimeoutException
     *             if the timeout has expired.
     */
    public void waitForSilence(long s, long timeoutMs)
            throws InterruptedException, TimeoutException {
        long remaining = timeoutMs; // amount of time remaining before timeout
                                    // expires.

        while (!isSilent(s) && (remaining > 0L)) { // keep trying while some
                                                   // time
                                                   // remains...

            long t = Math.min(remaining, remainingForSilence(s)); // can't
                                                                  // overshoot
                                                                  // timeout.
            remaining -= t;

            Thread.sleep(t);
        }

        if (!isSilent(s) && (remaining <= 0L)) {
            throw new TimeoutException("Timed out. Not silent at the end of "
                    + timeoutMs + " ms.");
        }
    }

    /**
     * Time that has elapsed since last call to tic().
     * 
     * @return elapsed time in ms, -1 if no action has occurred.
     */
    public long timeSinceLast() {
        long lt = lastTime;
        long ct = System.currentTimeMillis();

        return (lt == 0) ? Long.MAX_VALUE : (ct - lt);
    }
}