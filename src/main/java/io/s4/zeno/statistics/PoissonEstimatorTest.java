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
package io.s4.zeno.statistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

// TODO: Auto-generated Javadoc
/**
 * The Class PoissonEstimatorTest.
 */
public class PoissonEstimatorTest {

    /**
     * The main method.
     * 
     * @param arg
     *            the arguments
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void main(String[] arg) throws IOException {
        PoissonEstimator pe = new PoissonEstimator(0.05);

        while ((new BufferedReader(new InputStreamReader(System.in))).readLine() != null) {
            pe.putEvent();
            System.out.println("rate: " + pe.getRate());
        }

        System.out.println("Done. Final rate: " + pe.getRate());
    }
}
