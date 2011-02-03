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

import java.util.List;

/**
 * A list of parts out of which some may be acquired and released.
 * Particular Parts may be started, stopped, paused and unpaused.
 */
public interface PartList {
    List<Part.Id> acquire(int n);
    
    void release(Part.Id id);

    void markStarted(Part.Id id);

    void unmarkStarted(Part.Id id);

    void markPaused(Part.Id id);

    void unmarkPaused(Part.Id id);
    
    void markTakenOver(Part.Id id, Job job);
}