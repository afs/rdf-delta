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

package org.seaborne.delta;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.seaborne.delta.DeltaConst.symDeltaClient;
import static org.seaborne.delta.DeltaConst.symDeltaConnection;
import static org.seaborne.delta.DeltaConst.symDeltaZone;

import java.net.BindException;

import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.atlas.lib.FileOps;
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
import org.seaborne.delta.client.assembler.VocabDelta;
import org.seaborne.delta.lib.LogX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;

/**
 * Test DeltaAssembler assuming {@link ManagedDatasetBuilder} works correctly.
 * See {@link TestManagedDatasetBuilder} and {@link TestManagedDatasetBuilder2}.
 */
public class TestDeltaAssembler {
    private static final String DIR = "testing/assembler";

    @BeforeClass public static void setForTesting() {
        if ( System.getProperty("java.util.logging.configuration") == null )
            LogX.setJavaLogging("src/test/resources/logging.properties");
    }

    private static DeltaServer deltaServer = null;

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
        // Port number in the assemblers.
        deltaServer = DeltaServer.create(1069, deltaLink);
        deltaServer.start();
    }

    @After public void after() {
        deltaServer.stop();
        Zone.clearZoneCache();
    }

    @Test public void assembler_delta_1() {
        // In-memory
        Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-dataset-mem.ttl", DatasetAssembler.getType());
        assertNotNull(dataset.getContext().get(symDeltaZone));
        assertNotNull(dataset.getContext().get(symDeltaConnection));
        assertNotNull(dataset.getContext().get(symDeltaClient));
    }

    /*
     * Sequence: second test fails if DeltaConnection.TestModeNoAsync = false;
     * assembler_delta_2
     * assembler_delta_3
     * assembler_ext_good_1
     * assembler_ext_good_2
     */

    private static void protect(Runnable r) {
        boolean value = DeltaConnection.TestModeNoAsync;
        DeltaConnection.TestModeNoAsync = true;
        // Or it needs a long time (5 seconds) to finish cleanup.
        // Lib.sleep(5000);
        try {
            r.run();
        } finally {
            DeltaConnection.TestModeNoAsync = value;
        }
    }

    @Test public void assembler_delta_2() {
        protect(this::assembler_delta_2_work);
    }

    private void assembler_delta_2_work() {
        // TDB1
        Quad q1 = SSE.parseQuad("(:g :s :p 1)");
        Quad q2 = SSE.parseQuad("(:g :s :p 2)");
        {
            Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-dataset-tdb1.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connectionForDataset(dataset) ) {
                dataset.executeWrite(()->conn.getDatasetGraph().add(q1));
                dataset.executeRead(()->conn.getDatasetGraph().contains(q1));
            }
            Zone zone = (Zone)(dataset.getContext().get(symDeltaZone));
        }
        // Don't clear zone setup.
        // Build again.
        {
            Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-dataset-tdb1.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connectionForDataset(dataset) ) {
                dataset.executeWrite(()->assertTrue(conn.getDatasetGraph().contains(q1)));
            }
        }
    }

    @Test public void assembler_delta_3() {
        protect(this::assembler_delta_3_work);
    }

    private void assembler_delta_3_work() {
        // TDB2
        Quad q1 = SSE.parseQuad("(:g :s :p 1)");
        Quad q2 = SSE.parseQuad("(:g :s :p 2)");
        {
            // If not protected and second run, fails here.
            Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-dataset-tdb2.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connectionForDataset(dataset) ) {
                dataset.executeWrite(()->conn.getDatasetGraph().add(q1));
                dataset.executeRead(()->conn.getDatasetGraph().contains(q1));
            }
            Zone zone = (Zone)(dataset.getContext().get(symDeltaZone));
        }

        // Do clear zone setup.
        Zone.clearZoneCache();
        // Build again.
        {
            Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-dataset-tdb2.ttl", DatasetAssembler.getType());
            try ( DeltaConnection conn = connectionForDataset(dataset) ) {
                dataset.executeWrite(()->assertTrue(conn.getDatasetGraph().contains(q1)));
            }
        }
    }

    private static DeltaConnection connectionForDataset(Dataset dataset) {
        return (DeltaConnection)(dataset.getContext().get(symDeltaConnection));
    }

    // External
    // XXX External good 1 - all mem - fuseki-assembler-ext + resource type.
    //Need two zones? Windows-isms?

    @Test public void assembler_ext_good_1() {
        protect(this::assembler_ext_good_1_work);
    }

    private void assembler_ext_good_1_work() {
        Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-assembler-ext-good-1.ttl", VocabDelta.tDatasetDelta);
        try ( DeltaConnection conn = connectionForDataset(dataset) ) {
            dataset.executeRead(()->assertTrue(conn.getDatasetGraph().isEmpty()));
        }
    }

    @Test public void assembler_ext_good_2() {
        protect(this::assembler_ext_good_2_work);
    }

    private void assembler_ext_good_2_work() {
        Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-assembler-ext-good-2.ttl", VocabDelta.tDatasetDelta);
        try ( DeltaConnection conn = connectionForDataset(dataset) ) {
            Txn.executeRead(conn.getDatasetGraph(), ()->assertTrue(conn.getDatasetGraph().isEmpty()));
        }
    }

    @Test(expected = AssemblerException.class)
    public void assembler_ext_bad_1() {
        // No delta:storage, no delta:dataset.
        Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-assembler-ext-bad-1.ttl", VocabDelta.tDatasetDelta);
    }

    @Test(expected = AssemblerException.class)
    public void assembler_ext_bad_2() {
        // delta:dataset but delta:storage != external
        Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-assembler-ext-bad-2.ttl", VocabDelta.tDatasetDelta);
    }

    @Test(expected = AssemblerException.class)
    public void assembler_ext_bad_3() {
        // delta:storage = "external" but no delta:dataset.
        Dataset dataset = (Dataset)AssemblerUtils.build(DIR+"/delta-assembler-ext-bad-3.ttl", VocabDelta.tDatasetDelta);
    }
}
