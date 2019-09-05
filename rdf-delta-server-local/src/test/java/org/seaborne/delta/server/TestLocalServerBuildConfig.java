/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        assertEquals(Provider.LOCAL, conf.getLogProviderType());
    }

    @Test public void local_server_config_01() {
        LocalServer server = LocalServers.createFile(TESTING+"DeltaServerBlankFile");
        assertEquals(Provider.FILE, server.getPatchStore().getProvider().getType());
        assertTrue(server.getDataRegistry().isEmpty());
    }

    @Test public void local_server_config_02() {
        LocalServer server = LocalServers.createFromConf(TESTING+"DeltaServer/delta.cfg");
        assertEquals(Provider.LOCAL, server.getPatchStore().getProvider().getType());
    }

    @Test public void local_server_config_03() {
        String DIR = TESTING+"DeltaServerBlankDft";
        LocalServer server = LocalServers.createFromConf(DIR+"/delta.cfg");
        List<DataSource> sources = server.listDataSources();
        assertEquals(0, sources.size());
        assertEquals(Provider.LOCAL, server.getPatchStore().getProvider().getType());
    }
}
