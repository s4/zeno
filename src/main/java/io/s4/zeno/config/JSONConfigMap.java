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

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

// TODO: Auto-generated Javadoc
/**
 * The Class JSONConfigMap.
 */
public class JSONConfigMap implements WritableConfigMap {

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(JSONConfigMap.class);

    /** The config. */
    protected volatile JSONObject config = null;
    
    /**
     * Instantiates a new jSON config map.
     */
    public JSONConfigMap() {
    }

    /**
     * Instantiates a new jSON config map.
     * 
     * @param x
     *            the x
     */
    public JSONConfigMap(JSONConfigMap x) {
        this.config = x.config;
    }

    /**
     * Instantiates a new jSON config map.
     * 
     * @param config
     *            the config
     */
    public JSONConfigMap(JSONObject config) {
        this.config = config;
    }

    /**
     * Instantiates a new jSON config map.
     * 
     * @param configStr
     *            the config str
     */
    public JSONConfigMap(String configStr) {
        try {
            this.config = new JSONObject(configStr);

        } catch (JSONException e) {
            logger.error("error parsing json: " + e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#get(java.lang.String)
     */
    public String get(String key) {
        return get(key, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getList(java.lang.String)
     */
    public String[] getList(String key) {
        if (config == null) return new String[0];

        JSONArray ja = config.optJSONArray(key);
        if (ja == null) return new String[0];

        int n = ja.length();
        String[] a = new String[n];

        for (int i = 0; i < n; ++i) {
            a[i] = ja.optString(i);
        }

        return a;
    }

    /**
     * Gets the.
     * 
     * @param key
     *            the key
     * @param v
     *            the v
     * @return the string
     */
    public String get(String key, String v) {
        return (config == null ? v : config.optString(key, v));
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getInt(java.lang.String, int)
     */
    public int getInt(String key, int v) {
        return (config == null ? v : config.optInt(key, v));
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getIntList(java.lang.String)
     */
    public int[] getIntList(String key) {
        if (config == null) return new int[0];

        JSONArray ja = config.optJSONArray(key);
        if (ja == null) return new int[0];

        int n = ja.length();
        int[] a = new int[n];

        for (int i = 0; i < n; ++i) {
            a[i] = ja.optInt(i);
        }

        return a;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getDouble(java.lang.String, double)
     */
    public double getDouble(String key, double v) {
        return (config == null ? v : config.optDouble(key, v));
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getDoubleList(java.lang.String)
     */
    public double[] getDoubleList(String key) {
        if (config == null) return new double[0];

        JSONArray ja = config.optJSONArray(key);
        if (ja == null) return new double[0];

        int n = ja.length();
        double[] a = new double[n];

        for (int i = 0; i < n; ++i) {
            a[i] = ja.optDouble(i);
        }

        return a;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getLong(java.lang.String, long)
     */
    public long getLong(String key, long v) {
        return (config == null ? v : config.optLong(key, v));
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getLongList(java.lang.String)
     */
    public long[] getLongList(String key) {
        if (config == null) return new long[0];

        JSONArray ja = config.optJSONArray(key);
        if (ja == null) return new long[0];

        int n = ja.length();
        long[] a = new long[n];

        for (int i = 0; i < n; ++i) {
            a[i] = ja.optLong(i);
        }

        return a;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getBoolean(java.lang.String,
     * boolean)
     */
    public boolean getBoolean(String key, boolean v) {
        return (config == null ? v : config.optBoolean(key, v));
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.ConfigMap#getBooleanList(java.lang.String)
     */
    public boolean[] getBooleanList(String key) {
        if (config == null) return new boolean[0];

        JSONArray ja = config.optJSONArray(key);
        if (ja == null) return new boolean[0];

        int n = ja.length();
        boolean[] a = new boolean[n];

        for (int i = 0; i < n; ++i) {
            a[i] = ja.optBoolean(i);
        }

        return a;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return config.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.WritableConfigMap#set(java.lang.String,
     * java.lang.String)
     */
    public boolean set(String key, String value) {
        try {
            this.config.put(key, value);
            return true;

        } catch (JSONException e) {
            logger.error("error setting " + key + "=" + value + ": ", e);
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.s4.zeno.config.WritableConfigMap#remove(java.lang.String)
     */
    public boolean remove(String key) {
        this.config.remove(key);
        return true;
    }
    
    public boolean save() {
        // saving is not applicable here.
        return true;
    }

    public WritableConfigMap chroot(String root) {
        return new RootedWritableConfigMap(root, this);
    }
}
