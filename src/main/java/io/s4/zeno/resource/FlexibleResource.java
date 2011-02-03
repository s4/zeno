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
package io.s4.zeno.resource;

import io.s4.zeno.Resource;

// TODO: Auto-generated Javadoc
// A flexible estimate of resources.
// E.g.free memory estimate with lower/upper bounds.
/**
 * The Interface FlexibleResource.
 */
public interface FlexibleResource extends Resource {
    // can this resource be expanded?
    /**
     * Can expand.
     * 
     * @return true, if successful
     */
    boolean canExpand();

    // return an expanded versoin of this
    /**
     * Expand.
     * 
     * @return the flexible resource
     */
    FlexibleResource expand();

    // resource is almost empty. e.g. lower bound of free memory estimate is
    // zero.
    /**
     * Almost empty.
     * 
     * @return true, if successful
     */
    boolean almostEmpty();

    // duplicate
    /**
     * Duplicate.
     * 
     * @return the flexible resource
     */
    FlexibleResource duplicate();
}
