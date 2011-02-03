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
 * An abstract representation of resources. Examples include fraction of time
 * used for processing, CPU utilization, Memory usage, etc.
 */
public interface Resource extends Comparable<Resource> {

    /**
     * Checks if resource empty.
     * 
     * @return true, if empty
     */
    boolean isEmpty();

    /**
     * Tests if this resource is sufficient to satisfy the demanded resources. 
     * 
     * @param demand
     *            the demand
     * @return true, if demand can be met by this resource.
     */
    boolean canAccept(Resource demand);

    /**
     * Tests if this resource is sufficient to satisfy the demanded resources, at least partially. 
     * 
     * @param demand
     *            the demand
     * @return true, if demand can be met at least partially
     */
    boolean canAcceptPartial(Resource demand);

    /**
     * Reduce resource by a certain amount
     * 
     * @param r
     *            the amount which it is to be reduced.
     */
    void reduce(Resource r);

    /**
     * Add to resources.
     * 
     * @param r
     *            the amount to add.
     */
    void add(Resource r);

    /**
     * Represent resource as bytes.
     * 
     * @return byte array representation.
     */
    byte[] toBytes();
}
