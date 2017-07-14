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

package org.seaborne.delta;

import org.apache.jena.atlas.logging.LogCtl;
import org.junit.*;

public class TestRemoteClient extends AbstractTestDeltaClient {
    @BeforeClass public static void setForTesting() { 
        //LogCtl.setLog4j();
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }
    
    static Setup.LinkSetup setup = new Setup.RemoteSetup();
    
    @Override
    public Setup.LinkSetup getSetup() {
        return setup;
    }
    
    @BeforeClass public static void beforeClass()   { setup.beforeClass(); }
    @AfterClass  public static void afterClass()    { setup.afterClass(); }
    @Before public void beforeTest()                { setup.beforeTest(); }
    @After  public void afterTest()                 { setup.afterTest(); }
}
