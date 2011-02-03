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

/**
 * A load level and resource utilization monitor.
 */
public interface LoadMonitor {

    /**
     * Gets the load level. The returned value may be the result of a previous
     * call to {@link #detectLevel()}.
     * 
     * @return the load level
     */
    LoadLevel getLevel();

    /**
     * Detect and return load level.
     * 
     * @return the load level
     */
    LoadLevel detectLevel();

    /**
     * Detect resource utilization.
     * 
     * @return used resources.
     */
    Resource getResourceUsage();

    /**
     * Detect resource availability. This is related to {@link
     * #getExcessResourceUsage()}.
     * 
     * @return available resources
     */
    Resource getFreeResource();

    /**
     * Detect the amount by which resource usage exceeds a certain limit. This
     * is related to {@link #getFreeResource()}
     * 
     * @return excess resource usage
     */
    Resource getExcessResourceUsage();
}
