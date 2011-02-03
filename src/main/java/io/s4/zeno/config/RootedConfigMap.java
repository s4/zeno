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

public class RootedConfigMap implements ConfigMap {

    private final String root;
    private final ConfigMap c;

    public RootedConfigMap(String root, ConfigMap c) {
        this.root = root;
        this.c = c;
    }

    protected String k(String key) {
        return root + '.' + key;
    }

    @Override
    public String get(String key) {
        return c.get(k(key));
    }

    @Override
    public String[] getList(String key) {
        return c.getList(k(key));
    }

    @Override
    public int getInt(String key, int v) {
        return c.getInt(k(key), v);
    }

    @Override
    public int[] getIntList(String key) {
        return c.getIntList(k(key));
    }

    @Override
    public double getDouble(String key, double v) {
        return c.getDouble(k(key), v);
    }

    @Override
    public double[] getDoubleList(String key) {
        return c.getDoubleList(k(key));
    }

    @Override
    public long getLong(String key, long v) {
        return c.getLong(k(key), v);
    }

    @Override
    public long[] getLongList(String key) {
        return c.getLongList(k(key));
    }

    @Override
    public boolean getBoolean(String key, boolean v) {
        return c.getBoolean(k(key), v);
    }

    @Override
    public boolean[] getBooleanList(String key) {
        return c.getBooleanList(k(key));
    }
    
    @Override
    public ConfigMap chroot(String root) {
        return new RootedConfigMap(root, this);
    }
}