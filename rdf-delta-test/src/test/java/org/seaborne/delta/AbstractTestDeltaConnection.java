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
import static org.junit.Assert.assertNull ;
import static org.junit.Assert.assertTrue ;
import static org.junit.Assert.fail ;

import java.nio.file.Files ;
import java.nio.file.Path ;
import java.util.Set;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.ext.com.google.common.base.Objects;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.system.StreamRDFLib ;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.base.file.Location;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.client.DataState ;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Test a client connection over a link */  
public abstract class AbstractTestDeltaConnection {
    protected static final String FILES_DIR = DeltaTestLib.TDIR+"test_dconn/";
    
    @BeforeClass public static void setupZone() { 
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
        String DIR = "target/Zone";
        Location loc = Location.create(DIR);
        FileOps.ensureDir(DIR);
        Zone.get().init(loc);
    }
    
    @AfterClass public static void cleanOutZone() {
        Zone.get().shutdown();
    }
    
    public abstract Setup.LinkSetup getSetup();
    
    public DeltaLink getLink() { return getSetup().getLink(); }
    
    public Zone getZone() { return Zone.get(); }
    
    protected DeltaConnection create() {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        return create(dsg);
    }
    
    protected static final String DS_NAME = "dsTest";
    protected static final String DS_URI = "http://example/"+DS_NAME;

    protected DeltaConnection create(DatasetGraph shadow) {
        DeltaLink dLink = getLink();
        if ( ! dLink.isRegistered() ) {
            Id clientId = Id.create();
            RegToken regToken = dLink.register(clientId);
        }                
        Id clientId = dLink.getClientId();
        DeltaConnection dConn = DeltaConnection.create(getZone(), DS_NAME, DS_URI, shadow, dLink);
        return dConn;
    }
    
    protected DeltaConnection connect(Id dsRef, DatasetGraph shadow) {
        DeltaConnection dConn = DeltaConnection.connect(getZone(), dsRef, shadow, getLink());
        return dConn;
    }
    
    protected DeltaConnection attach(Id dsRef, DatasetGraph shadow) {
        DeltaConnection dConn = DeltaConnection.attach(getZone(), dsRef, shadow, getLink());
        return dConn;
    }

    @Test(expected=DeltaBadRequestException.class)
    public void connect_non_existing() {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        DatasetGraph shadow = null;
        Id dsRef = Id.create();
        DeltaConnection dConn = DeltaConnection.connect(getZone(), dsRef, shadow, dLink);
    }
    
    @Test
    public void create_dconn_0() {
        // Create on the Delta link then connect DeltaConnection.
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        String DS_NAME = "123";
        Id dsRef = dLink.newDataSource(DS_NAME, "http://example/datasource");
        DeltaConnection dConn = DeltaConnection.connect(getZone(), dsRef, /*shadow*/null, dLink);
    }

    @Test
    public void create_dconn_1() {
        // Create via DeltaConnection.
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        Id dsRef = Id.create();
        DeltaConnection dConn = DeltaConnection.create(getZone(), "create_dconn_1", /*shadow*/null, /*uri*/null, dLink);
    }

    @Test
    public void create_dconn_2() {
        // Create twice
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        DatasetGraph shadow = null;
        
        DeltaConnection dConn1 = DeltaConnection.create(getZone(), "create_dconn_2", /*shadow*/null, /*uri*/null, dLink);
        try {
            DeltaConnection dConn2 = DeltaConnection.create(getZone(), "create_dconn_2", /*shadow*/null, /*uri*/null, dLink);
            fail("Didn't get a DeltaBadRequestException");
        } catch (DeltaBadRequestException ex) {}
    }

    @Test
    public void create_dconn_3() {
        // Versions
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        DatasetGraph shadow = null;
        
        DeltaConnection dConn = DeltaConnection.create(getZone(), "create_dconn_3", /*shadow*/null, /*uri*/null, dLink);
        assertEquals(0, dConn.getLocalVersion());
        assertEquals(0, dConn.getRemoteVersionLatest());
    }

    @Test
    public void create_delete_dconn_1() {
        Id dsRef = null;
        try(DeltaConnection dConn = create()) {
            assertTrue(dConn.isValid());
            dsRef = dConn.getDataSourceId();
            dConn.removeDataSource();
            
            assertFalse(dConn.isValid());

            // Immediately gone. 
            assertFalse(getZone().exists(dsRef));
            
            DataState ds = getZone().get(dsRef);
            assertNull(ds);
        }
    }
    
    @Test
    public void create_delete_dconn_2() {
        Id dsRef = null;
        Path path = null;
        try(DeltaConnection dConn = create()) {
            assertTrue(dConn.isValid());
            dsRef = dConn.getDataSourceId();
            path = getZone().get(dsRef).getStatePath();
            dConn.removeDataSource();
            assertFalse(dConn.isValid());
        }
        
        assertFalse(getZone().exists(dsRef));
        DataState ds = getZone().get(dsRef);
        assertNull(ds);

        if ( path != null )
            assertFalse("Zone data state persistence", Files.exists(path));
    }

    @Test
    public void initial_data_1() {
        // Create twice
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        dLink.register(clientId);
        DeltaConnection dConn = DeltaConnection.create(getZone(), "NEW", /*shadow*/null, /*uri*/null, dLink);
        String url1 = dConn.getInitialStateURL();
        assertNotNull(url1);
        RDFDataMgr.parse(StreamRDFLib.sinkNull(), url1);
    }

    // Make a change, ensure the local dataset is changed. 
    @Test
    public void change_1() {
        try(DeltaConnection dConn = create()) {
            long verLocal0 = dConn.getLocalVersion();
            long verRemotel0 = dConn.getRemoteVersionLatest();
            
            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                dsg.add(SSE.parseQuad("(:gx :sx :px :ox)"));
            });
            
            int verLocal1 = dConn.getLocalVersion();
            int verRemotel1 = dConn.getRemoteVersionLatest();
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
        try(DeltaConnection dConn = create()) {
            Id dsRef = dConn.getDataSourceId();
            int version = dConn.getRemoteVersionLatest();

            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s1 :p1 :o1)");
                dsg.add(q);
            });
            // Rebuild directly.
            DatasetGraph dsg2 = DatasetGraphFactory.createTxnMem();
            int ver = dConn.getRemoteVersionLatest();
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
        try(DeltaConnection dConn = create()) {
//            Id dsRef = dConn.getDatasourceId();
//            int version = dConn.getRemoteVersionLatest();
    
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
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
    
        int verLocal = -999;
        int verRemote = -999;
        Id dsRef;
        
        try(DeltaConnection dConn = create(dsgBase)) {
            dsRef = dConn.getDataSourceId();
            dsgBase = dConn.getStorage();
            DatasetGraph dsg = dConn.getDatasetGraph();
            int ver = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(0, ver);
            // Make change.
            Txn.executeWrite(dsg, ()->dsg.add(quad));
            verLocal = dConn.getLocalVersion();
            assertEquals(ver+1, verLocal);
        }
        
        betweenSections.run();
        
        // Reconnect
        try(DeltaConnection dConn = connect(dsRef, dsgBase)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            int ver = dConn.getLocalVersion();
            int ver2 = dConn.getRemoteVersionLatest();
    
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
        Id dsRef = null;
        
        try(DeltaConnection dConn = create()) {
            dsRef = dConn.getDataSourceId();
            int version = dConn.getRemoteVersionLatest();
            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->dsg.add(quad) );
        }

        betweenSections.run();

        DatasetGraph dsg2 = DatasetGraphFactory.createTxnMem();
        try(DeltaConnection dConn = attach(dsRef, dsg2)) {
            boolean b = dsg2.contains(quad);
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
    
        int verLocal = -999;
        int verRemote = -999;
        Id dsRef;
        
        try(DeltaConnection dConn = create(dsgBase)) {
            dsRef = dConn.getDataSourceId();
            dsgBase = dConn.getStorage();
            DatasetGraph dsg = dConn.getDatasetGraph();
            int ver = dConn.getLocalVersion();
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
        try(DeltaConnection dConn = connect(dsRef, dsgBase)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            
            Txn.executeWrite(dsg, ()->dsg.add(quad2));
            
            int ver = dConn.getLocalVersion();
            int ver2 = dConn.getRemoteVersionLatest();
            
            assertEquals(verLocal+1, ver);
            assertEquals(verRemote+1, ver2);
            
            verLocal = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
        }
    
        betweenSections2.run();
        
        // Reconnect and read
        try(DeltaConnection dConn = connect(dsRef, dsgBase)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            int ver = dConn.getLocalVersion();
            int ver2 = dConn.getRemoteVersionLatest();
    
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
//        int verLocal = -999;
//        int verRemote = -999;
//        Id dsRef;
//        
//        try(DeltaConnection dConn = create(dsgBase)) {
//            dsRef = dConn.getDatasourceId();
//            dsgBase = dConn.getStorage();
//            DatasetGraph dsg = dConn.getDatasetGraph();
//            int ver = dConn.getLocalVersionNumber();
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
//            int ver = dConn.getLocalVersionNumber();
//            int ver2 = dConn.getRemoteVersionLatest();
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
//            int ver = dConn.getLocalVersionNumber();
//            int ver2 = dConn.getRemoteVersionLatest();
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
