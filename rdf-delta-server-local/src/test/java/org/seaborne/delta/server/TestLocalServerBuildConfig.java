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

package org.seaborne.delta.server;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.Delta;
import org.seaborne.delta.server.local.*;

/**
 *  Tests of configuration and building a {@link LocalServer}.
 */
public class TestLocalServerBuildConfig {
    
    // This is the pre-setup testing area and is not modified in tests.
    public static String TESTING = "testing/";

    @BeforeClass public static void beforeClass() {
        Delta.init();
    }
    
    @Before public void before() {
//        DPS.resetSystem();
        DPS.init();
    }
    
    @Test public void local_server_config_builder_01() {
        // Server configuration builder.
        LocalServerConfig conf = LocalServerConfig.create()
            .parse(TESTING+"delta.cfg")
            .build();
    }

    @Test public void local_server_config_01() {
        LocalServer server = LocalServers.createFile(TESTING+"DeltaServerBlank");
    }

    @Test public void local_server_config_02() {
        LocalServers.createFile(TESTING+"DeltaServer");
    }

    @Test public void local_server_config_03() {
        String DIR = TESTING+"DeltaServerBlank";
        LocalServer server = LocalServers.createConf(DIR+"/delta.cfg");
        List<DataSource> sources = server.listDataSources();
        assertEquals(0, sources.size());
    }
}
