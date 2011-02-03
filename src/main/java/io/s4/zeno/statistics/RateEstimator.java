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
package io.s4.zeno.statistics;

/**
 * Estimate event arrival rate.
 */
public interface RateEstimator {

    /**
     * Signal that an event has arrived at the current time. Calling this method
     * updates the state opf the estimator.
     */
    void putEvent();

    /**
     * Gets the current estimate of the event rate assuming that a hypothetical
     * event arrives at the time when this function is called. This hypothetical
     * last event is not used to update the state of the estimator.
     * 
     * @return the rate
     */
    double getRate();

    /**
     * Gets the time that has elapsed since last event arrived. I.e. the last call to {@link #putEvent()}.
     * 
     * @return milliseconds since last event.
     */
    long getMillisSinceLastEvent();
}
