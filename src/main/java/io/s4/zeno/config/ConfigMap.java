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
 * A key-value map. The keys are strings. The values may be interpreted as a
 * variety of types: see the getters for all options. It is recommended that the
 * keys be structured hierarchically with dots ({@code .}) as separators, like
 * java package names. This allows the creation of maps with a subspace of keys.
 */
public interface ConfigMap {

    /**
     * Gets the.
     * 
     * @param key
     *            the key
     * @return the string
     */
    String get(String key);

    /**
     * Gets the list.
     * 
     * @param key
     *            the key
     * @return the list
     */
    String[] getList(String key);

    /**
     * Gets the int.
     * 
     * @param key
     *            the key
     * @param v
     *            the v
     * @return the int
     */
    int getInt(String key, int v);

    /**
     * Gets the int list.
     * 
     * @param key
     *            the key
     * @return the int list
     */
    int[] getIntList(String key);

    /**
     * Gets the double.
     * 
     * @param key
     *            the key
     * @param v
     *            the v
     * @return the double
     */
    double getDouble(String key, double v);

    /**
     * Gets the double list.
     * 
     * @param key
     *            the key
     * @return the double list
     */
    double[] getDoubleList(String key);

    /**
     * Gets the long.
     * 
     * @param key
     *            the key
     * @param v
     *            the v
     * @return the long
     */
    long getLong(String key, long v);

    /**
     * Gets the long list.
     * 
     * @param key
     *            the key
     * @return the long list
     */
    long[] getLongList(String key);

    /**
     * Gets the boolean.
     * 
     * @param key
     *            the key
     * @param v
     *            the v
     * @return the boolean
     */
    boolean getBoolean(String key, boolean v);

    /**
     * Gets the boolean list.
     * 
     * @param key
     *            the key
     * @return the boolean list
     */
    boolean[] getBooleanList(String key);

    /**
     * To string.
     * 
     * @return the string
     */
    public String toString();

    public ConfigMap chroot(String root);
}
