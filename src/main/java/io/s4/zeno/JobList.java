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
 * A list of Jobs out of which one may be acquired and released, typically at a
 * Site.
 * 
 * Example: A list of jobs for ZooKeeper-based clusters.
 */
public interface JobList {
    /**
     * Acquire a job, if possible.
     * 
     * @return acquired job. Null i fno job could be acquired.
     */
    Job acquire();

    /**
     * Release a job to list.
     * 
     * @param job
     */
    void release(Job job);

    /**
     * Acquire a job, standing by for one to become available if necessary.
     * 
     * @return acquired job.
     */
    Job standbyAcquire();
}
