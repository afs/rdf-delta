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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.server.local.*;

/**
 * Tests of {@link LocalServer} on a preconfigured LocalServer area. See
 * {@link TestLocalServerCreateDelete} for tests involving create and delete of data
 * sources.
 */
public class TestLocalServer {
    
    // This is the pre-setup testing area and is not modified in tests.
    public static String SERVER_DIR = "testing/DeltaServer";
    
    @BeforeClass public static void beforeClass() {
        DPS.resetSystem();
    }

    @Test public void local_server_01() {
        LocalServer server = LocalServers.createFile(SERVER_DIR);
        List<DataSource> sources = server.listDataSources();
        assertEquals(2, sources.size());
        test(sources.get(0));
        test(sources.get(1));
    }

    private void test(DataSource dataSource) {
        dataSource.getId();
        dataSource.getPatchLog();
        dataSource.getURI();
    }
}
