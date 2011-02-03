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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * A service which can be scheduled to run periodically in a thread pool.
 */
public abstract class Service {
    private static final Logger logger = Logger.getLogger(Service.class);

    /**
     * The work done by this service goes in here.
     */
    abstract protected void action();

    public Service() {
        this.serviceName = this.getClass().getName();
    }

    /**
     * Named service
     * 
     * @param name
     *            name of service.
     */
    public Service(String name) {
        this.serviceName = name;
    }

    private final String serviceName;

    /**
     * Get the name of this service.
     * 
     * @return service name
     */
    public final String serviceName() {
        return serviceName;
    }

    // representation of this action after it has been scheduled for execution.
    private volatile ScheduledFuture<?> scheduledAction = null;

    // executor.
    private ScheduledThreadPoolExecutor scheduler;

    /**
     * Start this service to be run by an executor. The periodicity and initial
     * delay can be controlled by the {@link #delay()} and
     * {@link #initialDelay()} methods.
     * 
     * Before the first execution of the service action, the service may be
     * initialized by implementing the {@link #initialize()} method.
     * 
     * @param scheduler
     *            executor which will run this service.
     */
    public final synchronized void start(ScheduledThreadPoolExecutor scheduler) {
        if (active) return;

        this.scheduler = scheduler;

        initialize();

        active = true;

        logger.info("scheduling service " + serviceName);

        scheduledAction = scheduler.scheduleWithFixedDelay(actionRunner,
                                                           initialDelay,
                                                           delay + 10,
                                                           TimeUnit.MILLISECONDS);
    }

    /**
     * Stop this service. In case this service runs a long lasting action (e.g.
     * blocking reads), it is possible to interrupt the action by implementing
     * the {@link Service#unblock()} method, which is called before the service
     * is removed from the executor.
     */
    public final synchronized void stop() {
        if (!active) return;

        active = false;
        unblock();

        scheduledAction.cancel(false);

        scheduler.remove(actionRunner);
    }

    // delay before first action
    private long initialDelay = 5000;

    /**
     * Get the delay before the first execution of the action.
     * 
     * @return delay in milliseconds
     */
    public long initialDelay() {
        return initialDelay;
    }

    /**
     * Set the delay before the first execution of the action. If the delay must
     * be randomized, use {@link #setInitialDelay(long, long)}.
     * 
     * @param initialDelay
     *            delay in milliseconds
     */
    public void setInitialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
    }

    /**
     * Set the delay before the first execution of the action. The delay is not
     * exact, but rather a random value in the range {@code [d0, d1]}.
     * 
     * @param d0
     *            lower bound of delay in milliseconds
     * 
     * @param d1
     *            upper bound of delay in milliseconds
     * 
     * @return actual (random) delay in milliseconds.
     */
    public long setInitialDelay(long d0, long d1) {
        double d = d0 + Math.random() * (d1 - d0);
        return (this.initialDelay = Math.round(d));
    }

    // interval between actions
    private long delay = 5000;

    /**
     * Time interval between consecutive executions of action.
     * 
     * @return delay in milliseconds
     */
    public long delay() {
        return delay;
    }

    /**
     * Set delay between consecutive executions of action.
     * 
     * @param delay
     *            delay in milliseconds
     */
    public void setDelay(long delay) {
        this.delay = delay;

        if (active && (scheduledAction != null)) {
            synchronized (this) {
                scheduledAction.cancel(false);
                scheduledAction = scheduler.scheduleWithFixedDelay(actionRunner,
                                                                   initialDelay,
                                                                   delay + 10,
                                                                   TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Initialization procedure which is run once when the service is started.
     */
    protected void initialize() {
    }

    /**
     * cleanup to be performed after the service is stopped.
     */
    protected void cleanup() {
    }

    /**
     * Procedure to make the action stop whatever it is doing.
     */
    protected void unblock() {
    }

    /**
     * The number of services that can coexist in the thread on which this
     * service runs. Default=10. This is used to estimate the thread pool size
     * for running a collection of services.
     * 
     * @return number of services in thread.
     */
    public int share() {
        return 10;
    }

    // is this service active to be scheduled?
    private volatile boolean active = false;

    // run the service's action() if it is active
    private class Action implements Runnable {
        public void run() {
            logger.debug("running action: " + serviceName);
            if (active) action();
            logger.debug("done action: " + serviceName);
        }
    }

    private Runnable actionRunner = new Action();
}
