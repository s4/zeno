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

import io.s4.zeno.util.ActivityMonitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;


/**
 * A collection of named services, counters, locks, and monitors. Allows
 * services to be started and stopped together.
 */
public class SiteRegistry {

    private final Logger logger = Logger.getLogger(SiteRegistry.class);

    /**
     * Start all registered services. The services are started in the order of
     * registration.
     */
    public synchronized void startServices() {
        if (started == false) {
            if (scheduler == null) {
                int sz = estimatePoolSize();
                scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(sz);
                logger.info("Creating Scheduler with pool size " + sz);
            }

            for (String n : serviceList) {
                logger.info("starting service: " + n);
                services.get(n).start(scheduler);
            }
        }

        started = true;
    }

    /**
     * Stop all services. The services are stopped in the reverse order of
     * registration.
     */
    public synchronized void stopServices() {
        if (started == true) {
            List<String> rlist = new ArrayList<String>(serviceList);
            Collections.reverse(rlist);

            for (String n : rlist) {
                services.get(n).stop();
            }
        }

        started = false;
    }

    /**
     * Register a service. Note that services will be started in the order in
     * which they are registered; they will be stopped in the reverse of this
     * order. The registry must be in the stopped state, and the name must not
     * be already registered.
     * 
     * @param name
     *            name to use for the Service
     * @param service
     *            the Service to register.
     * @return true if successfully registered.
     */
    public synchronized boolean registerService(String name, Service service) {
        if (started == false) {
            if (services.containsKey(name) == false) {
                services.put(name, service);
                serviceList.add(name);
                return true;
            } else {
                logger.error("service already registered for " + name);
            }
        } else {
            logger.error("cannot register services on a start-ed registry. Should stop-ped.");
        }
        return false;
    }

    /**
     * Deregister a service from the registry. The registry must be stopped
     * first.
     * 
     * @param name
     *            name of the Service0 to remove from the registry.
     * @return reference to the Service0 that was removed. {@code null} if the
     *         name was not found or the registry is in started state.
     */
    public synchronized Service deregisterService(String name) {
        if (started == false) {
            if (services.containsKey(name) == true) {
                serviceList.remove(name);
                return services.remove(name);
            } else {
                logger.error("service already registered for " + name);
            }
        } else {
            logger.error("cannot deregister services on a start-ed registry. Should stop-ped first.");
        }
        return null;
    }

    /**
     * Lookup a Service given a name.
     * 
     * @param name
     *            name of the service.
     * @return Service corresponding to {@code name}. {@code null} if not found.
     */
    public Service lookupService(String name) {
        return services.get(name);
    }

    private boolean started = false;

    // LOCKS
    /**
     * Obtain a named lock. This method blocks.
     * 
     * @param name
     *            name of the lock to be obtained.
     */
    public void lock(String name) {
        ReentrantLock l = locks.getOrCreate(name);
        l.lock();
    }

    /**
     * Try to obtain a named lock. This function returns immediately.
     * 
     * @param name
     *            name of the lock to be obtained.
     * @return true if the lock was obtained. False otherwise.
     */
    public boolean tryLock(String name) {
        ReentrantLock l = locks.getOrCreate(name);
        return l.tryLock();
    }

    /**
     * Release a named lock.
     * 
     * @param name
     *            name of the lock to be released.
     */
    public void unlock(String name) {
        ReentrantLock l = locks.getOrCreate(name);
        l.unlock();
    }

    /**
     * Obtain a lock and run the specified action. Then unlock. This method
     * blocks till the lock has been obtained.
     * 
     * @param l
     *            name of the lock to be obtained
     * @param r
     *            action to be run.
     */
    public void lockAndRun(String l, Runnable r) {
        logger.info("acquire lock " + l + " then run");

        // first obtain the lock
        this.lock(l);

        // then try to run r
        try {
            r.run();

        } finally {
            // no matter what, release the lock at the end.
            this.unlock(l);
        }
    }

    /**
     * Try to obtain a lock and run the specified action. Unlocks when done. If
     * the lock is not free to be obtained when this method is called, then the
     * action is not performed and the method returns immediately. For a
     * blocking version, use {@link #lockAndRun(String, Runnable)}.
     * 
     * @param l
     *            name of the lock to be obtained.
     * @param r
     *            action to run.
     * @return true if and only if the action was run.
     */
    public boolean tryLockAndRun(String l, Runnable r) {
        logger.info("trying to acquire lock " + l + " then run");
        if (this.tryLock(l)) {
            try {
                // run the action if the lock was acquired.
                r.run();
                return true;

            } finally {
                // make sure we unlock.
                this.unlock(l);
            }
        } else {
            // could not get the lock, so action was not run.
            return false;
        }
    }

    // COUNTERS
    /**
     * Get count from a named counter.
     * 
     * @param name
     *            name of counter.
     */
    public int getCount(String name) {
        AtomicInteger c = counters.getOrCreate(name);
        return c.intValue();
    }

    /**
     * Set count of a named counter, getting previously held value.
     * 
     * @param name
     *            name of counter
     * @param count
     *            value to set to counter.
     * @return previously held value.
     */
    public int setCount(String name, int count) {
        AtomicInteger c = counters.getOrCreate(name);
        return c.getAndSet(count);
    }

    /**
     * Add an integer to a counter, getting previously held value.
     * 
     * @param name
     *            name of counter.
     * @param delta
     *            value to add. Use a negative value for decrementing.
     * @return previously held value.
     */
    public int addCount(String name, int delta) {
        AtomicInteger c = counters.getOrCreate(name);
        return c.getAndAdd(delta);
    }

    // Activity Monitors
    public ActivityMonitor getActivityMonitor(String name) {
        return monitors.getOrCreate(name);
    }

    // estimate size of thread pool required
    // for running the registered services
    private int estimatePoolSize() {
        float count = 0.0F;

        for (Service s : services.values()) {
            // number of services with which this service can coexist.
            float share = s.share();

            if (share >= 0.0) {
                // 1/shared ~ fraction of thread required for this service.
                count += 1.0 / share;
            }
        }

        // round up to whole number of threads.
        return (int) Math.ceil(count);
    }

    private final ArrayList<String> serviceList = new ArrayList<String>();
    private ScheduledThreadPoolExecutor scheduler = null;

    private final ConcurrentHashMap<String, Service> services = new ConcurrentHashMap<String, Service>();

    private final InstantiableMap<ReentrantLock> locks = new InstantiableMap<ReentrantLock>(new Factory<ReentrantLock>() {
        public ReentrantLock create() {
            return new ReentrantLock(true);
        }
    });

    private final InstantiableMap<AtomicInteger> counters = new InstantiableMap<AtomicInteger>(new Factory<AtomicInteger>() {
        public AtomicInteger create() {
            return new AtomicInteger();
        }
    });

    private final InstantiableMap<ActivityMonitor> monitors = new InstantiableMap<ActivityMonitor>(new Factory<ActivityMonitor>() {
        public ActivityMonitor create() {
            return new ActivityMonitor();
        }
    });

    /**
     * A ConcurrentHashMap with string keys and the functionality to instantiate
     * values for non-existing keys.
     */
    private class InstantiableMap<T> extends ConcurrentHashMap<String, T> {

        private static final long serialVersionUID = 6806277769457116222L;

        private final Factory<T> factory;

        public InstantiableMap(Factory<T> factory) {
            super();

            this.factory = factory;
        }

        public T getOrCreate(String s) {

            T t = this.get(s);

            if (t != null) return t;

            // have to create a new instance.
            T nt = factory.create();

            // try to add it into the hash-map
            t = this.putIfAbsent(s, nt);

            // t == null => new value was added to map
            // t != null => someone added value before us. t is that value
            return (t == null ? nt : t);
        }
    }

    private interface Factory<T> {
        public T create();
    }
}