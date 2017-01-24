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

/**
 *  Tests of {@link LocalServer} on a preconfigured LocalServer area.
 *  See {@link TestLocalServer2} for tests involving create and delete
 *  of data sources.
 */
public class TestLocalServer1 {
    
    // This is the pre-setup testing area and is not modified in tests.
    public static String SERVER_DIR = "testing/delta";

    @Test public void local_server_01() {
        // Pre-setup.
        Location loc = Location.create(SERVER_DIR);
        LocalServer server = LocalServer.attach(loc, "delta.cfg");
        List<DataSource> sources = server.listDataSources();
        assertEquals(2, sources.size());
        test(sources.get(0));
        test(sources.get(1));
    }

    private void test(DataSource dataSource) {
        dataSource.getId();
        dataSource.getPatchSet();
        dataSource.getReceiver();
        dataSource.getURI();
    }
}
