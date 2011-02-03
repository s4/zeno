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

import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

// TODO: Auto-generated Javadoc
/**
 * The Class ZenoDefs.
 */
public class ZenoDefs {

    /** The Constant emptyBytes. */
    public static final byte[] emptyBytes = {};

    /** The Constant zkACL. */
    public static final List<ACL> zkACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    /** The Constant emptyString. */
    public static final String emptyString = "";
}