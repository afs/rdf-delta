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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.client.DeltaClient;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.TxnSyncPolicy;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;

public class TestZone {
    static String DIR_ZONE = "target/Zone2";

    private Zone zone;
    private DeltaLink deltaLink;
    private DeltaClient deltaClient;

    @BeforeClass public static void beforeClass() { 
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }
    
    @Before public void before() {
        FileOps.ensureDir(DIR_ZONE);
        FileOps.clearAll(DIR_ZONE);
        zone = Zone.create(DIR_ZONE);

        LocalServer lserver = DeltaTestLib.createEmptyTestServer();
        deltaLink = DeltaLinkLocal.connect(lserver);
        deltaLink.register(Id.create());
        zone = Zone.create(DIR_ZONE);
        deltaClient = DeltaClient.create(zone, deltaLink);
    }

    private Id createExternal(String name, DatasetGraph dsgBase) {
        Id dsRef = deltaClient.newDataSource(name, "http://example/"+name);
        deltaClient.attachExternal(dsRef, dsgBase);
        deltaClient.connect(dsRef, TxnSyncPolicy.NONE);
        return dsRef;
    }
    
    @After public void after() {
        deltaLink.close();
        zone.shutdown();
    }
    
    @AfterClass public static void afterClass() {
    }
    
    @Test public void zone_01() {
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
        String NAME = "ABC"; 
        Id dsRef = createExternal(NAME, dsgBase);

        try(DeltaConnection dConn = deltaClient.get(dsRef)) {
            DatasetGraph storage = dConn.getStorage();
            assertEquals(dsgBase, storage);
        }
    }

    @Test public void zone_02() {
        assertTrue(zone.localConnections().isEmpty());
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
        String NAME = "ABC"; 
        Id dsRef = createExternal(NAME, dsgBase);
        assertFalse(zone.localConnections().isEmpty());
        Quad quad = SSE.parseQuad("(_ :s :p :o)");
        
        try(DeltaConnection dConn = deltaClient.get(dsRef)) {
            DatasetGraph dsg  = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->dsg.add(quad)); 
        }
        
        try(DeltaConnection dConn = deltaClient.get(dsRef)) {
            DatasetGraph dsg  = dConn.getDatasetGraph();
            Txn.executeRead(dsg, ()->dsg.contains(quad)); 
        }
    }
    
    @Test public void zone_03() {
        assertTrue(zone.localConnections().isEmpty());
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
        String NAME = "ABC"; 
        Id dsRef = createExternal(NAME, dsgBase);
        assertFalse(zone.localConnections().isEmpty());
        Quad quad = SSE.parseQuad("(_ :s :p :o)");
        try(DeltaConnection dConn = deltaClient.get(dsRef)) {
            DatasetGraph dsg  = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->dsg.add(quad)); 
        }
        // read log.
        PatchLogInfo info = deltaLink.getPatchLogInfo(dsRef);
        assertEquals(1, info.getMaxVersion());
    }

    @Test public void zone_graph_external_1() {
        // TODO
        assertTrue(zone.localConnections().isEmpty());
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
        String NAME = "ABC"; 
        Id dsRef = createExternal(NAME, dsgBase);
        assertFalse(zone.localConnections().isEmpty());
        Quad quad = SSE.parseQuad("(_ :s :p :o)");
        try(DeltaConnection dConn = deltaClient.get(dsRef)) {
            DatasetGraph dsg  = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->dsg.add(quad)); 
        }
        // read log.
        PatchLogInfo info = deltaLink.getPatchLogInfo(dsRef);
        assertEquals(1, info.getMaxVersion());
    }


}
