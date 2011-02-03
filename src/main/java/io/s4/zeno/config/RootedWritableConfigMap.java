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

public class RootedWritableConfigMap extends RootedConfigMap implements
        WritableConfigMap {

    private final WritableConfigMap w;

    public RootedWritableConfigMap(String root, WritableConfigMap w) {
        super(root, w);
        this.w = w;
    }

    @Override
    public boolean set(String key, String value) {
        return w.set(k(key), value);
    }

    @Override
    public boolean remove(String key) {
        return w.remove(k(key));
    }

    @Override
    public boolean save() {
        return w.save();
    }

    public WritableConfigMap chroot(String root) {
        return new RootedWritableConfigMap(root, this);
    }
}
