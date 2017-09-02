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

import static org.junit.Assert.assertEquals ;
import static org.junit.Assert.assertFalse ;
import static org.junit.Assert.assertNotNull ;
import static org.junit.Assert.assertTrue ;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger ;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.ext.com.google.common.base.Objects;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.client.*;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Test a client connection over a link */  
public abstract class AbstractTestDeltaConnection {
    // See also AbstractTestDeltaClient
    
    private static String DIR = "target/Zone";

    @BeforeClass public static void setupZone() { 
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
        FileOps.ensureDir(DIR);
        FileOps.clearAll(DIR);
        Zone.create(DIR);
    }
    
    @AfterClass public static void cleanOutZone() {
        Zone.get(DIR).shutdown();
    }
    
    protected abstract Setup.LinkSetup getSetup();
    
    protected DeltaLink getLink() {
        DeltaLink dLink = getSetup().getLink() ;
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        return dLink;
    }

    protected DeltaLink getLinkUnregister() {
        return getSetup().getLink() ;
    }    
    
    protected Zone getZone() { return Zone.get(DIR); }
    
    protected DeltaClient createDeltaClient() {
        return DeltaClient.create(getZone(), getLink());  
    }
    
    protected DeltaClient createRegister(String name) {
        DeltaClient dClient = DeltaClient.create(getZone(), getLink());
        Id dsRef = dClient.newDataSource(name, "http://example/"+name);
        dClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.NONE);
        return dClient;
    }
    
    // Connection create.
    
    @Test
    public void create_dconn_1() {
        DeltaClient dClient = createDeltaClient();

        String DS_NAME = "create_dconn_1";
        String DS_URI = "http://example/"+DS_NAME;

        // This attaches it as well.
        Id dsRef = dClient.newDataSource(DS_NAME, DS_URI);
        dClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.NONE);
        DeltaConnection dConn = dClient.get(dsRef);
        assertNotNull(dConn.getDatasetGraph());
        assertEquals(0, dConn.getLocalVersion());
        assertEquals(0, dConn.getRemoteVersionLatest());

        Id dsRef1 = dConn.getDataSourceId();
        assertEquals(dsRef, dsRef1);

        PatchLogInfo info = dConn.getPatchLogInfo();

        assertEquals(DS_NAME, info.getDataSourceName());
        assertEquals(dsRef, info.getDataSourceId());
    }
    
    // Make a change, ensure the local dataset is changed. 
    @Test
    public void change_1() {
        String NAME = "change_1s";
        DeltaClient dClient = createRegister(NAME);
        
        try(DeltaConnection dConn = dClient.get(NAME)) {
            long verLocal0 = dConn.getLocalVersion();
            long verRemotel0 = dConn.getRemoteVersionLatest();
            
            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                dsg.add(SSE.parseQuad("(:gx :sx :px :ox)"));
            });
            
            long verLocal1 = dConn.getLocalVersion();
            long verRemotel1 = dConn.getRemoteVersionLatest();
            assertEquals(verLocal1, dConn.getLocalVersion());
            assertEquals(verRemotel1, dConn.getRemoteVersionLatest());
            
            assertFalse(dConn.getDatasetGraph().isEmpty());
            if ( dConn.getStorage() != null )
                assertFalse(dConn.getStorage().isEmpty());
        }
    }

    // Make a change, get the patch, apply to a clean dsg. Are the datasets the same?
    @Test
    public void change_2() {
        String NAME = "change_2";
        DeltaClient dClient = createRegister(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            Id dsRef = dConn.getDataSourceId();
            long version = dConn.getRemoteVersionLatest();

            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s1 :p1 :o1)");
                dsg.add(q);
            });
            // Rebuild directly.
            DatasetGraph dsg2 = DatasetGraphFactory.createTxnMem();
            long ver = dConn.getRemoteVersionLatest();
            RDFPatch patch1 = dConn.getLink().fetch(dsRef, ver) ;
            RDFPatchOps.applyChange(dsg2, patch1);

            Set<Quad> set1 = Txn.calculateRead(dsg, ()->Iter.toSet(dsg.find()));
            Set<Quad> set2 = Txn.calculateRead(dsg2, ()->Iter.toSet(dsg2.find()));
            assertEquals(set1, set2);
        }
    }
    
    // Make two changes in one DeltaConnection.
    @Test
    public void change_change_1() {
        String NAME = "change_2";
        DeltaClient dClient = createRegister(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s1 :p1 :o1)");
                dsg.add(q);
            });
    
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s2 :p2 :o2)");
                dsg.add(q);
            });

            long c = Txn.calculateRead(dsg, ()->Iter.count(dsg.find()));
            assertEquals(2,c);
        }
    }

    // XXX More cases of change-restart inc new zone. 
    
    // ---- Same dataset carried across connections
    
    @Test public void change_read_same_1() {
        change_read_same(()->{});
    }

    @Test public void change_read_same_2() {
        change_read_same(()->getSetup().relink());
    }

    @Test public void change_read_same_3() {
        change_read_same(()->getSetup().restart());
    }

    private static AtomicInteger counter = new AtomicInteger(0); 
    private DeltaClient resetDeltaClient(String name) {
        DeltaClient dClient = createDeltaClient();
        Id dsRef = dClient.nameToId(name);
        if ( ! dClient.getZone().exists(dsRef) )
            dClient.register(dsRef, LocalStorageType.MEM, TxnSyncPolicy.NONE);
        else
            dClient.connect(dsRef, TxnSyncPolicy.NONE);
        return dClient;
    }

    /** 
     * Make change.
     * Some kind of reset.
     * Reconnect to the same server and see if the versions reflect the change.
     * Same dataset.
     */
    private void change_read_same(Runnable betweenSections) {
        // Make change.
        // Reconnect to the same server and see if the versions reflect the change.
        Quad quad = DeltaTestLib.freshQuad();
        DatasetGraph dsgBase;
    
        long verLocal = -999;
        long verRemote = -999;
        Id dsRef;
        
        String NAME = "DS-"+counter.incrementAndGet();
        
        DeltaClient dClient = createRegister(NAME); 
        try(DeltaConnection dConn = dClient.get(NAME)) {
            dsRef = dConn.getDataSourceId();
            dsgBase = dConn.getStorage();
            DatasetGraph dsg = dConn.getDatasetGraph();
            long ver = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(0, ver);
            // Make change.
            Txn.executeWrite(dsg, ()->dsg.add(quad));
            verLocal = dConn.getLocalVersion();
            assertEquals(ver+1, verLocal);
        }
        
        betweenSections.run();
        
        // New client.
        dClient = resetDeltaClient(NAME);
        // Same name.
        // Zone should have found (or not lost) the existing setup. 
        // dClient.connect(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            long ver = dConn.getLocalVersion();
            long ver2 = dConn.getRemoteVersionLatest();
    
            assertEquals(verLocal, ver);
            assertEquals(verLocal, ver2);
            boolean b = Txn.calculateRead(dsg, ()->dsg.contains(quad));
            assertTrue(b);
        }
    }

    // ---- Different dataset each connection. 
    
    @Test
    public void change_read_new_1() {
        change_read_new(()->{});
    }

    @Test
    public void change_read_new_2() {
        change_read_new(()->getSetup().relink());
    }

    @Test
    public void change_read_new_3() {
        change_read_new(()->getSetup().restart());
    }

    // ---- Different dataset each connection. 
    
    // Update, reset, read.
    private void change_read_new(Runnable betweenSections) {
        Quad quad = DeltaTestLib.freshQuad();
        
        String NAME = "DS-"+counter.incrementAndGet();
        DeltaClient dClient = createRegister(NAME);
        Id dsRef;
        
        try(DeltaConnection dConn = dClient.get(NAME)) {
            dConn.getPatchLogInfo().getDataSourceId();
            dsRef = dConn.getDataSourceId();
            long version = dConn.getRemoteVersionLatest();
            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->dsg.add(quad) );
        }

        betweenSections.run();

        // New client.
        // Rebuild.
        dClient = resetDeltaClient(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            boolean b = dConn.getDatasetGraph().contains(quad);
            assertTrue(b);
        }
    }
    
    @Test public void change_change_read_same_1() {
        change_change_read_Same(()->{});
    }

    @Test public void change_change_read_same_2() {
        change_change_read_Same(()->getSetup().relink());
    }

    @Test public void change_change_read_same_3() {
        change_change_read_Same(()->getSetup().restart());
    }

    /** Make change.
     * Reset
     * Reconnect and make another change.
     * Reset
     * Reconnect to the same server and see if the versions reflect the change.
     * Same dataset.
     */
    private void change_change_read_Same(Runnable betweenSections) {
        change_change_read_Same(betweenSections, betweenSections);
    }

    private void change_change_read_Same(Runnable betweenSections1, Runnable betweenSections2) {
        Quad quad1 = DeltaTestLib.freshQuad();
        Quad quad2 = DeltaTestLib.freshQuad();
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
    
        long verLocal = -999;
        long verRemote = -999;
        Id dsRef;
        
        String NAME = "DS-"+counter.incrementAndGet();
        DeltaClient dClient = createRegister(NAME);        
        try(DeltaConnection dConn = dClient.get(NAME)) {
            dsRef = dConn.getDataSourceId();
            dsgBase = dConn.getStorage();
            DatasetGraph dsg = dConn.getDatasetGraph();
            long ver = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(0, ver);
            // Make change.
            Txn.executeWrite(dsg, ()->dsg.add(quad1));
            verLocal = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(ver+1, verLocal);
        }
        
        betweenSections1.run();
        
        // Reconnect, make second change.
        dClient = resetDeltaClient(NAME);

        try(DeltaConnection dConn = dClient.get(NAME)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            
            Txn.executeWrite(dsg, ()->dsg.add(quad2));
            
            long ver = dConn.getLocalVersion();
            long ver2 = dConn.getRemoteVersionLatest();
            
            assertEquals(verLocal+1, ver);
            assertEquals(verRemote+1, ver2);
            
            verLocal = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
        }
    
        betweenSections2.run();
        
        // Reconnect and read
        dClient = resetDeltaClient(NAME); 
        try(DeltaConnection dConn = dClient.get(NAME)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            long ver = dConn.getLocalVersion();
            long ver2 = dConn.getRemoteVersionLatest();
    
            assertEquals(verLocal, ver);
            assertEquals(verLocal, ver2);
            boolean b = Txn.calculateRead(dsg, ()-> dsg.contains(quad1)&&dsg.contains(quad2) );
            assertTrue(b);
        }
    }
    
//    @Test public void change_change_read_new_1() {
//        createChangeChangeReadNew(()->{});
//    }
//
//    @Test public void change_change_read_new_2() {
//        createChangeChangeReadNew(()->getSetup().relink());
//    }
//
//    @Test public void change_change_read_new_3() {
//        createChangeChangeReadNew(()->getSetup().restart());
//    }
//
//    /** Make change.
//     * Reset
//     * Reconnect and make another change.
//     * Reset
//     * Reconnect to the same server and see if the versions reflect the change.
//     * Different dataset.
//     */
//    private void createChangeChangeReadNew(Runnable betweenSections) {
//        createChangeChangeReadNew(betweenSections, betweenSections);
//    }
//
//    private void createChangeChangeReadNew(Runnable betweenSections1, Runnable betweenSections2) {
//        Quad quad1 = DeltaTestLib.freshQuad();
//        Quad quad2 = DeltaTestLib.freshQuad();
//        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
//    
//        long verLocal = -999;
//        long verRemote = -999;
//        Id dsRef;
//        
//        try(DeltaConnection dConn = create(dsgBase)) {
//            dsRef = dConn.getDatasourceId();
//            dsgBase = dConn.getStorage();
//            DatasetGraph dsg = dConn.getDatasetGraph();
//            long ver = dConn.getLocalVersionNumber();
//            verRemote = dConn.getRemoteVersionLatest();
//            assertEquals(0, ver);
//            // Make change.
//            Txn.executeWrite(dsg, ()->dsg.add(quad1));
//            verLocal = dConn.getLocalVersionNumber();
//            assertEquals(ver+1, verLocal);
//        }
//        
//        betweenSections1.run();
//        
//        // Reconnect, make second change.
//        try(DeltaConnection dConn = connect(dsRef, dsgBase)) {
//            DatasetGraph dsg = dConn.getDatasetGraph();
//            
//            Txn.executeWrite(dsg, ()->dsg.add(quad2));
//            
//            long ver = dConn.getLocalVersionNumber();
//            long ver2 = dConn.getRemoteVersionLatest();
//            
//            assertEquals(verLocal+1, ver);
//            assertEquals(verRemote+1, ver2);
//            
//            verLocal = dConn.getLocalVersionNumber();
//            verRemote = dConn.getRemoteVersionLatest();
//        }
//    
//        betweenSections2.run();
//        
//        // Reconnect and read
//        try(DeltaConnection dConn = connect(dsRef, dsgBase)) {
//            DatasetGraph dsg = dConn.getDatasetGraph();
//            long ver = dConn.getLocalVersionNumber();
//            long ver2 = dConn.getRemoteVersionLatest();
//    
//            assertEquals(verLocal, ver);
//            assertEquals(verLocal, ver2);
//            boolean b = Txn.calculateRead(dsg, ()-> dsg.contains(quad1)&&dsg.contains(quad2) );
//            assertTrue(b);
//        }
//    }

    private static boolean equals(RDFPatch patch1, RDFPatch patch2) {
        RDFChangesCollector c1 = new RDFChangesCollector();
        // The getRDFPatch is a RDFPatchStored which supports hashCode and equals.
        RDFChangesCollector.RDFPatchStored p1 = (RDFChangesCollector.RDFPatchStored)c1.getRDFPatch();
        
        RDFChangesCollector c2 = new RDFChangesCollector();
        RDFChangesCollector.RDFPatchStored p2 = (RDFChangesCollector.RDFPatchStored)c2.getRDFPatch();
        
        return Objects.equal(p1, p2);
    }
}
