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

import static org.junit.Assert.*;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.base.file.Location;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.client.DeltaClient;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.client.TxnSyncPolicy;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.patchlog.FileStore;
import org.seaborne.delta.server.local.patchlog.PatchStore;

// There are restart tests in AbstractTestsDeltaConnection as well.
// Here we are concerned with different ways to restart on the client side
// such as extenr datasets, deleted remote while inactive etc.
public class TestRestart {
    static String DIR_ZONE = "target/Zone3";
    static Location ServerArea = Location.create("target/test/server");
    static String TDIR = "testing/";
    
    private Zone zone;
    private LocalServer localServer; 
    private DeltaLink deltaLink;
    private DeltaClient deltaClient;

    @Before public void before() {
        ensureClear(DIR_ZONE);
        ensureClear(ServerArea.getDirectoryPath());
    }
    
    @After public void after() {
        shutdown();
    }

    private static void ensureClearLocal() {
        ensureClear(DIR_ZONE);
    }
        
    private static void ensureClearRemote() {   
        ensureClear(ServerArea.getDirectoryPath());
        FileStore.resetTracked();
        PatchStore.clearPatchLogs();
    }
    
    private static void ensureClear(String area) {
        FileOps.ensureDir(area);
        FileOps.clearAll(area);
    }
    
    //DeltaTestLib.
    
    
    private static void test(Id dsRef, DeltaClient dClient, int numQuads) {
        try(DeltaConnection dConn = dClient.get(dsRef)) {
            DatasetGraph dsg  = dConn.getDatasetGraph();
            Txn.executeRead(dsg, ()->{
                assertEquals(numQuads, Iter.count(dsg.find()));
            });
        }
    }
    
    private static void update(Id dsRef, DeltaClient dClient) {
        // Use it.
        Quad quad = quad();
        try(DeltaConnection dConn = dClient.get(dsRef)) {
            DatasetGraph dsg  = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->dsg.add(quad)); 
        }
        
        try(DeltaConnection dConn = dClient.get(dsRef)) {
            DatasetGraph dsg  = dConn.getDatasetGraph();
            Txn.executeRead(dsg, ()->{
                assertTrue(dsg.contains(quad));
            });
        }
    }

    private static int counter = 0;

    // Unique quad.
    private static Quad quad() { return SSE.parseQuad("(_ :s :p "+(counter++)+")"); }

    private void setupEmpty() {
        ensureClearLocal();
        ensureClearRemote();
        String cfg = "delta.cfg";
        IOX.copy(TDIR+cfg, ServerArea.getDirectoryPath()); 
        setup();
    }
    
    private void setup() {
        localServer = LocalServer.attach(ServerArea);
        deltaLink = DeltaLinkLocal.connect(localServer);
        deltaLink.register(Id.create());
        zone = Zone.create(DIR_ZONE);
        deltaClient = DeltaClient.create(zone, deltaLink);
    }

    private void shutdown() {
        //deltaLink.close();
        if ( zone != null )
            zone.shutdown();
        if ( localServer != null )
            localServer.shutdown();
        FileStore.resetTracked();
        PatchStore.clearPatchLogs();
        zone = null;
        localServer = null;
        deltaClient = null;
        deltaLink = null;
    }
    
    private Id createExternal(String name, DatasetGraph dsgBase) {
        Id dsRef = deltaClient.newDataSource(name, "http://example/"+name);
        deltaClient.attachExternal(dsRef, dsgBase);
        deltaClient.connect(dsRef, TxnSyncPolicy.NONE);
        return dsRef;
    }

    @BeforeClass public static void beforeClass() { 
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }
    
    @AfterClass public static void afterClass() {
        
    }

    @Test public void restart_00() {
        setupEmpty();
        assertTrue(zone.localConnections().isEmpty());
        assertTrue(deltaClient.getZone().localConnections().isEmpty());
        assertTrue(deltaLink.listDatasets().isEmpty());

        shutdown();
        setup();
        assertTrue(zone.localConnections().isEmpty());
        assertTrue(deltaClient.getZone().localConnections().isEmpty());
        assertTrue(deltaLink.listDatasets().isEmpty());
    }
    
    @Test public void lifecycle_01() {
        setupEmpty();
        assertTrue(zone.localConnections().isEmpty());
        String NAME = "lifecycle_01";
        Id dsRef = deltaClient.newDataSource(NAME, "http://example/"+NAME);
        
        DataSourceDescription dsd = deltaLink.getDataSourceDescription(dsRef);
        assertNotNull(dsd);
        
        // Remote only
        assertEquals(0, zone.localConnections().size());
        assertNull(deltaClient.get(dsRef));
        
        // Not connected, but in zone.
        deltaClient.attach(dsRef, LocalStorageType.MEM);
        zone.exists(dsRef);
        assertNull(deltaClient.get(dsRef));
        
        // Connected!
        deltaClient.connect(dsRef, TxnSyncPolicy.TXN_RW);
        assertNotNull(deltaClient.get(dsRef));

        test(dsRef, deltaClient, 0);
        update(dsRef, deltaClient);
        
    }

    @Test public void lifecycle_02() {
        // Like lifecycle_01 but use deltaClient.register
        setupEmpty();
        String NAME = "lifecycle_02";
        Id dsRef = deltaClient.newDataSource(NAME, "http://example/"+NAME);
        // Use "register"
        deltaClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.TXN_RW);
        assertNotNull(deltaClient.get(dsRef));
        test(dsRef, deltaClient, 0);
        update(dsRef, deltaClient);
    }

    @Test public void lifecycle_03() {
        // Like lifecycle_01 but use deltaClient.register
        setupEmpty();
        String NAME = "lifecycle_03";
        Id dsRef = deltaClient.newDataSource(NAME, "http://example/"+NAME);
        deltaClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.TXN_RW);
        assertNotNull(deltaClient.get(dsRef));
        test(dsRef, deltaClient, 0);
        update(dsRef, deltaClient);
        deltaClient.removeDataSource(dsRef);
        assertFalse(deltaClient.existsLocal(dsRef));
        // Exists remote.
        assertFalse(deltaClient.existsRemote(dsRef));
    }

    
    @Test public void restart_01() {
        setupEmpty();
        String NAME = "restart_01";
        Id dsRef = deltaClient.newDataSource(NAME, "http://example/"+NAME);
        deltaClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.TXN_RW);
        
        test(dsRef, deltaClient, 0);
        update(dsRef, deltaClient);
        
        shutdown();
        setup();
        
        assertTrue(deltaClient.existsLocal(dsRef));
        deltaClient.connect(dsRef, TxnSyncPolicy.TXN_RW);
        
        test(dsRef, deltaClient, 1);
    }
    
    @Test public void restart_02() {
        setupEmpty();
        String NAME = "restart_02";
        Id dsRef = deltaClient.newDataSource(NAME, "http://example/"+NAME);
        deltaClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.TXN_RW);
        update(dsRef, deltaClient);

        shutdown();
        
        // Clear local.
        ensureClearLocal();
        setup();
        
        assertFalse(deltaClient.existsLocal(dsRef));
        // Exists remote.
        assertTrue(deltaClient.existsRemote(dsRef));
        
        PatchLogInfo info = deltaClient.getLink().getPatchLogInfo(dsRef);
        assertEquals(1, info.getMinVersion());
        assertEquals(1, info.getMaxVersion());
        
        deltaClient.attach(dsRef, LocalStorageType.MEM);
        deltaClient.connect(dsRef, TxnSyncPolicy.NONE);
        test(dsRef, deltaClient, 1);
    }
    
    //existing local, deleted remote.
    @Test public void restart_03() {
        setupEmpty();
        String NAME = "restart_03";
        Id dsRef = deltaClient.newDataSource(NAME, "http://example/"+NAME);
        deltaClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.TXN_RW);
        update(dsRef, deltaClient);

        shutdown();
        
        // Clear remote
        ensureClearRemote();
        setup();
        
        assertTrue(deltaClient.existsLocal(dsRef));
        // Exists remote.
        assertFalse(deltaClient.existsRemote(dsRef));
        
        // Delete
        deltaClient.removeDataSource(dsRef);

        assertFalse(deltaClient.existsLocal(dsRef));
        assertFalse(deltaClient.existsRemote(dsRef));

//        PatchLogInfo info = deltaClient.getLink().getPatchLogInfo(dsRef);
//        assertEquals(1, info.getMinVersion());
//        assertEquals(1, info.getMaxVersion());
//        
//        deltaClient.attach(dsRef, LocalStorageType.MEM);
//        deltaClient.connect(dsRef, TxnSyncPolicy.NONE);
//        test(dsRef, deltaClient, 1);
    }
    
    @Test public void restart_04() {
        setupEmpty();
        String NAME = "restart_04";
        Id dsRef = deltaClient.newDataSource(NAME, "http://example/"+NAME);
        deltaClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.TXN_RW);
        assertNotNull(deltaClient.get(dsRef));
        test(dsRef, deltaClient, 0);
        update(dsRef, deltaClient);
        deltaClient.removeDataSource(dsRef);
        shutdown();
        setup();
        assertFalse(deltaClient.existsLocal(dsRef));
        assertFalse(deltaClient.existsRemote(dsRef));
    }

}
