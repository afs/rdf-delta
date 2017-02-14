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

import java.util.List;

import org.apache.jena.tdb.base.file.Location;
import org.junit.Test;
import static org.junit.Assert.*;
import org.seaborne.delta.server.local.DataSource;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;

/**
 *  Tests of configutation and building a {@link LocalServer}.
 */
public class TestLocalServerBuildConfig {
    
    // This is the pre-setup testing area and is not modified in tests.
    public static String TESTING = "testing/";

    @Test public void local_server_config_builder_01() {
        // Server configuration builder.
        LocalServerConfig conf = LocalServerConfig.create()
            .parse(TESTING+"delta.cfg")
            .build();
        Location x = Location.create(TESTING);
        assertEquals(x, conf.location);
        assertEquals(5050, conf.port);
    }

    @Test public void local_server_config_01() {
        // Blank start up.
        LocalServerConfig conf = LocalServerConfig.create()
            .setPort(10)
            .setLocation("target/test_config01")
            .build();
        LocalServer.attach(conf);
    }

    @Test public void local_server_config_02() {
        // Configuration file not in server area. 
        LocalServerConfig conf = LocalServerConfig.create()
            .parse(TESTING+"delta.cfg")
            .setLocation("target/test_config03")
            .build();
        LocalServer server = LocalServer.attach(conf);
    }

    @Test public void local_server_config_04() {
        // Configuration file in server area. 
        Location loc = Location.create(TESTING+"DeltaServerBlank");
        LocalServer server = LocalServer.attach(loc, "delta.cfg");
        List<DataSource> sources = server.listDataSources();
        assertEquals(0, sources.size());
    }
}
