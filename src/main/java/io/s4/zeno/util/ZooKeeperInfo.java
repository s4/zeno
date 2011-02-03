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
package io.s4.zeno.util;

import io.s4.zeno.config.ZKPaths;
import io.s4.zeno.config.ZKWritableConfigMap;
import io.s4.zeno.coop.DistributedSequence;
import io.s4.zeno.coop.NonblockingLockset;

/**
 * The Class ZooKeeperInfo.
 */
public class ZooKeeperInfo {

    /** The zookeeper. */
    public ZooKeeperHelper zookeeper = null;

    /** The task holder. */
    public NonblockingLockset taskHolder = null;

    /** The standby sequence. */
    public DistributedSequence standbySequence = null;

    /** The parts holder. */
    public NonblockingLockset partsHolder = null;

    /** The zkpath. */
    public ZKPaths zkpath = null;

    /** The node info. */
    public ZKWritableConfigMap nodeInfo = null;
}