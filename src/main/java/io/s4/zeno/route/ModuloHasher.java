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
package io.s4.zeno.route;

import io.s4.zeno.Part;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;


public class ModuloHasher implements Hasher {
    private static final Logger logger = Logger.getLogger(ModuloHasher.class);

    private volatile HashMap<Integer, Integer> groupSize = new HashMap<Integer, Integer>();

    @Override
    public Part.Id hash(int group, int key) {
        Integer g = groupSize.get(group);

        if (g != null) {
            return new Part.Id(group, key % g.intValue());
        }

        return null;
    }

    @Override
    public void rebuild(List<Part.Id> partIds) {
        HashMap<Integer, Integer> gsz = new HashMap<Integer, Integer>();

        for (Part.Id id : partIds) {
            Integer g = id.group;
            int k = id.key;

            if (!gsz.containsKey(g) || (gsz.get(g).intValue() <= k)) {
                gsz.put(g, k+1);
            }
        }

        groupSize = gsz;
        logger.debug(gsz);
    }
}
