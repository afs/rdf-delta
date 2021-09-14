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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.seaborne.delta.DeltaConst.symDeltaClient;
import static org.seaborne.delta.DeltaConst.symDeltaConnection;
import static org.seaborne.delta.DeltaConst.symDeltaZone;

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.dboe.base.file.Location;
import org.junit.After;
import org.junit.Before;
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
 * In-memory testing for {@link ManagedDatasetBuilder}. See
 * {@link TestManagedDatasetBuilder2} for persistent state tests.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestManagedDatasetBuilder {

//    @BeforeClass public static void setForTesting() {
//        LogX.setJavaLogging("src/test/resources/logging.properties");
//    }

    private DeltaLink deltaLink;
    private LocalServer localServer;
    @Before public void before() {
        LocalServerConfig config = LocalServers.configMem();
        localServer = LocalServer.create(config);
        deltaLink = DeltaLinkLocal.connect(localServer);
    }

    @After public void after() {
        LocalServer.releaseAll();
    }

    @Test public void buildManaged_1_basic() {
        DatasetGraph dsg = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            .logName("ABC")
            .zone(Zone.connect(Location.mem()))
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();
        assertNotNull(dsg.getContext().get(symDeltaZone));
        assertNotNull(dsg.getContext().get(symDeltaConnection));
        assertNotNull(dsg.getContext().get(symDeltaClient));

        DeltaConnection conn = (DeltaConnection)(dsg.getContext().get(symDeltaConnection));
        assertSame(deltaLink, conn.getLink());
        assertSame(dsg, conn.getDatasetGraph());
    }

    @Test public void buildManaged_2_sameZone() {
        Zone zone = Zone.connect(Location.mem());
        DatasetGraph dsg1 = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            .logName("ABC")
            .zone(zone)
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();
        // Rebuild from with same zone. Should get the same underlying storage database.
        // i.e. same Zone item.

        DeltaLink deltaLink2 = DeltaLinkLocal.connect(localServer);
        DatasetGraph dsg2 = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink2)
            .logName("ABC")
            .zone(zone)
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();

        assertNotSame(dsg1, dsg2);
        DeltaConnection conn1 = (DeltaConnection)(dsg1.getContext().get(symDeltaConnection));
        DeltaConnection conn2 = (DeltaConnection)(dsg2.getContext().get(symDeltaConnection));
        // Same zone, same underlying storage client-side
        assertSame(conn1.getStorage(), conn2.getStorage());

        Quad q1 = SSE.parseQuad("(:g :s :p 1)");
        Quad q2 = SSE.parseQuad("(:g :s :p 2)");
        Txn.executeWrite(dsg1, ()->dsg1.add(q1));
        Txn.executeRead(dsg2, ()->assertTrue(dsg2.contains(q1)));

        try ( DeltaConnection c = conn1 ) {
            DatasetGraph dsg  = c.getDatasetGraph();
            Txn.executeRead(  c.getDatasetGraph(), ()->assertTrue(c.getDatasetGraph().contains(q1)) );
            Txn.executeWrite( c.getDatasetGraph(), ()->c.getDatasetGraph().add(q2) );
        }
        try ( DeltaConnection c = conn2 ) {
            Txn.executeRead(c.getDatasetGraph(), ()->assertTrue(c.getDatasetGraph().contains(q2)));
        }
    }

    @Test public void buildManaged_3_differentZones() {
        // Different zones, same local server.
        Zone zone1 = Zone.connect(Location.mem());
        Zone zone2 = Zone.connect(Location.mem());
        DatasetGraph dsg1 = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            .logName("BCD")
            .zone(zone1)
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();
        // Different zones.

        DeltaLink deltaLink2 = DeltaLinkLocal.connect(localServer);
        DatasetGraph dsg2 = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink2)
            .logName("BCD")
            .zone(zone2)
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();

        Quad q1 = SSE.parseQuad("(:g :s :p 1)");
        Quad q2 = SSE.parseQuad("(:g :s :p 2)");
        Txn.executeWrite(dsg1, ()->dsg1.add(q1));
        Txn.executeRead(dsg2, ()->assertTrue(dsg2.contains(q1)));

        DeltaConnection conn1 = (DeltaConnection)(dsg1.getContext().get(symDeltaConnection));
        DeltaConnection conn2 = (DeltaConnection)(dsg2.getContext().get(symDeltaConnection));

        assertNotSame(conn1.getStorage(), conn2.getStorage());

        try ( DeltaConnection c = conn1 ) {
            DatasetGraph dsg  = c.getDatasetGraph();
            Txn.executeRead(  c.getDatasetGraph(), ()->assertTrue(dsg2.contains(q1)) );
            Txn.executeWrite( c.getDatasetGraph(), ()->c.getDatasetGraph().add(q2) );
        }
        try ( DeltaConnection c = conn2 ) {
            Txn.executeRead(c.getDatasetGraph(), ()->assertTrue(dsg2.contains(q2)));
        }
    }

    // Bad builds.

    @Test(expected=DeltaConfigException.class)
    public void buildManaged_bad_1_noDeltalLink() {
        DatasetGraph dsg = ManagedDatasetBuilder.create()
            //.deltaLink(deltaLink)
            .logName("ABC")
            .zone(Zone.connect(Location.mem()))
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();
    }

    @Test(expected=DeltaConfigException.class)
    public void buildManaged_bad_2_noLogName() {
        DatasetGraph dsg = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            //.logName("ABC")
            .zone(Zone.connect(Location.mem()))
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();
    }

    @Test(expected=DeltaConfigException.class)
    public void buildManaged_bad_3_noZone() {
        DatasetGraph dsg = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            .logName("ABC")
            //.zone(Zone.connect(Location.mem()))
            .syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();
    }

    @Test(expected=DeltaConfigException.class)
    public void buildManaged_bad_4_noSyncPolicy() {
        DatasetGraph dsg = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            .logName("ABC")
            .zone(Zone.connect(Location.mem()))
            //.syncPolicy(SyncPolicy.TXN_RW)
            .storageType(LocalStorageType.MEM)
            .build();
    }

    @Test(expected=DeltaConfigException.class)
    public void buildManaged_bad_5_noStorageType() {
        DatasetGraph dsg = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            .logName("ABC")
            .zone(Zone.connect(Location.mem()))
            .syncPolicy(SyncPolicy.TXN_RW)
            //.storageType(LocalStorageType.MEM)
            .build();
    }
}
