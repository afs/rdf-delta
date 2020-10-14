/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.integration;

import org.junit.*;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.systemtest.Matrix;

/** Additional tests for Zookeeper based RDF Delta */
public class TestZkExtra {

    @BeforeClass public static void beforeClass() {}
    @AfterClass public static void afterClass() {}

    @Before public void before() { Matrix.setup(); }
    @After  public void after()  { Matrix.teardown(); }

    @Test public void twoLinks() {
        DeltaLink dLink1 = Matrix.deltaServerLink1;
        DeltaLink dLink2 = Matrix.deltaServerLink2;

        Id dsRef1 = dLink1.newDataSource("ABC01", "http://example/ABC01");
        dLink1.removeDataSource(dsRef1);


        Id dsRef2 = dLink1.newDataSource("ABC02", "http://example/ABC02");
        dLink1.removeDataSource(dsRef2);
    }
}
