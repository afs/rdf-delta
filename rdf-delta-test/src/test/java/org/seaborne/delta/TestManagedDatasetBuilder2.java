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

import static org.junit.Assert.assertTrue;
import static org.seaborne.delta.DeltaConst.symDeltaConnection;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.base.file.Location;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.client.SyncPolicy;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.client.assembler.ManagedDatasetBuilder;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;

/**
 * Persistent state testing of {@link ManagedDatasetBuilder}. See
 * {@link TestManagedDatasetBuilder} for other tests.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestManagedDatasetBuilder2 {

    @BeforeClass public static void setForTesting() {
        if ( System.getProperty("java.util.logging.configuration") == null )
            LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }

    @BeforeClass public static void cleanStart() {
        FileOps.ensureDir(ZONE_DIR);
        FileOps.clearAll(ZONE_DIR);

        FileOps.ensureDir(DELTA_DIR);
        FileOps.clearAll(DELTA_DIR);
    }

    private static final String ZONE_DIR = "target/Zone" ;
    private static final String DELTA_DIR = "target/DeltaServer" ;

    private DeltaLink deltaLink;
    private LocalServer localServer;

    @Before public void before() {
        LocalServerConfig config = LocalServers.configFile(DELTA_DIR);
        localServer = LocalServer.create(config);
        deltaLink = DeltaLinkLocal.connect(localServer);
    }

    @After public void after() {
        LocalServer.releaseAll();
    }

    // Bit of an uber test but overheads are high.
    @Test public void buildManaged_persistentZone() {
        // Restart.
        Location loc = Location.create(ZONE_DIR);
        Zone zone = Zone.connect(loc);

        Quad q1 = SSE.parseQuad("(:g :s :p 1)");
        Quad q2 = SSE.parseQuad("(:g :s :p 2)");

        {
            DatasetGraph dsg = ManagedDatasetBuilder.create()
                .deltaLink(deltaLink)
                .logName("ABD")
                .zone(zone)
                .syncPolicy(SyncPolicy.TXN_RW)
                .storageType(LocalStorageType.TDB2)
                .build();

            DeltaConnection conn = (DeltaConnection)(dsg.getContext().get(symDeltaConnection));
            Txn.executeWrite(dsg, ()->dsg.add(q1));
            Txn.executeRead(  conn.getDatasetGraph(), ()->assertTrue(conn.getDatasetGraph().contains(q1)) );
        }

        // Same zone
        Zone.clearZoneCache();
        zone = Zone.connect(loc);
        // Storage should be recovered from the on-disk state.

        {
            DatasetGraph dsg1 = ManagedDatasetBuilder.create()
                .deltaLink(deltaLink)
                .logName("ABD")
                .zone(zone)
                .syncPolicy(SyncPolicy.TXN_RW)
                //.storageType(LocalStorageType.TDB2) // Storage required. Should detect [FIXME]
                .build();
            DatasetGraph dsg2 = ManagedDatasetBuilder.create()
                .deltaLink(deltaLink)
                .logName("ABD")
                .zone(zone)
                .syncPolicy(SyncPolicy.TXN_RW)
                //.storageType(LocalStorageType.TDB) // Wrong storage - does not matter; dsg1 setup choice applies
                .build();
            DeltaConnection conn1 = (DeltaConnection)(dsg1.getContext().get(symDeltaConnection));
            DeltaConnection conn2 = (DeltaConnection)(dsg2.getContext().get(symDeltaConnection));

            Txn.executeRead(conn1.getDatasetGraph(), ()->assertTrue(conn1.getDatasetGraph().contains(q1)) );
            Txn.executeWrite(conn2.getDatasetGraph(), ()->conn2.getDatasetGraph().add(q2));
            Txn.executeRead(conn1.getDatasetGraph(), ()->assertTrue(conn1.getDatasetGraph().contains(q2)) );
        }
    }
}
