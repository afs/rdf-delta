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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.seaborne.delta.DeltaConst.symDeltaClient;
import static org.seaborne.delta.DeltaConst.symDeltaConnection;
import static org.seaborne.delta.DeltaConst.symDeltaZone;

import java.net.BindException;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.core.assembler.DatasetAssembler;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.client.assembler.ManagedDatasetBuilder;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.PatchLogServer;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;

/**
 * Test DeltaAssembler assuming {@link ManagedDatasetBuilder} works correctly.
 * See {@link TestManagedDatasetBuilder} and {@link TestManagedDatasetBuilder2}.
 */
public class TestDeltaAssembler {
    @BeforeClass public static void setForTesting() {
        if ( System.getProperty("java.util.logging.configuration") == null )
            LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }

    private static PatchLogServer patchLogServer = null;

    @BeforeClass public static void beforeSetupDirs()  {
        FileOps.ensureDir("target/Zone1");
        FileOps.ensureDir("target/Zone2");
    }

    @BeforeClass public static void beforeClean()  {
        FileOps.clearAll("target/Zone1");
        FileOps.clearAll("target/Zone2");
    }
    @Before public void before() throws BindException {
        LocalServerConfig config = LocalServers.configMem();
        LocalServer localServer = LocalServer.create(config);
        DeltaLink deltaLink = DeltaLinkLocal.connect(localServer);
        patchLogServer = PatchLogServer.create(1068, deltaLink);
        patchLogServer.start();
    }

    @After public void after() {
        patchLogServer.stop();
    }

    @Test public void assembler_delta_1() {
        // In-memory
        Dataset dataset = (Dataset)AssemblerUtils.build("testing/delta-dataset-mem.ttl", DatasetAssembler.getType());
        assertNotNull(dataset.getContext().get(symDeltaZone));
        assertNotNull(dataset.getContext().get(symDeltaConnection));
        assertNotNull(dataset.getContext().get(symDeltaClient));
    }

    @Test public void assembler_delta_2() {
        // TDB1
        Quad q1 = SSE.parseQuad("(:g :s :p 1)");
        Quad q2 = SSE.parseQuad("(:g :s :p 2)");
        {
            Dataset dataset = (Dataset)AssemblerUtils.build("testing/delta-dataset-tdb1.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connection(dataset) ) {
                Txn.executeWrite(conn.getDatasetGraph(), ()->conn.getDatasetGraph().add(q1));
            }
            Zone zone = (Zone)(dataset.getContext().get(symDeltaZone));
        }
        // Don't clear zone setup.
        // Build again.
        {
            Dataset dataset = (Dataset)AssemblerUtils.build("testing/delta-dataset-tdb1.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connection(dataset) ) {
                Txn.executeRead(conn.getDatasetGraph(), ()->assertTrue(conn.getDatasetGraph().contains(q1)));
            }
        }
    }

    @Test public void assembler_delta_3() {
        // TDB2
        Quad q1 = SSE.parseQuad("(:g :s :p 1)");
        Quad q2 = SSE.parseQuad("(:g :s :p 2)");
        {
            Dataset dataset = (Dataset)AssemblerUtils.build("testing/delta-dataset-tdb2.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connection(dataset) ) {
                Txn.executeWrite(conn.getDatasetGraph(), ()->conn.getDatasetGraph().add(q1));
            }
            Zone zone = (Zone)(dataset.getContext().get(symDeltaZone));
        }

        // Do clear zone setup.
        Zone.clearZoneCache();
        // Build again.
        {
            Dataset dataset = (Dataset)AssemblerUtils.build("testing/delta-dataset-tdb2.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connection(dataset) ) {
                Txn.executeRead(conn.getDatasetGraph(), ()->assertTrue(conn.getDatasetGraph().contains(q1)));
            }
        }

    }

    private static DeltaConnection connection(Dataset dataset) {
        return (DeltaConnection)(dataset.getContext().get(symDeltaConnection));
    }
}
