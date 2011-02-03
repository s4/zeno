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
package io.s4.zeno.config;

// TODO: Auto-generated Javadoc
/**
 * The Interface WritableConfigMap.
 */
public interface WritableConfigMap extends ConfigMap {

    /**
     * Sets the.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     * @return true, if successful
     */
    public boolean set(String key, String value);

    /**
     * Removes the.
     * 
     * @param key
     *            the key
     * @return true, if successful
     */
    public boolean remove(String key);

    /**
     * Save the map to a backing store, if one exists.
     */
    public boolean save();
    
    public WritableConfigMap chroot(String root);
}