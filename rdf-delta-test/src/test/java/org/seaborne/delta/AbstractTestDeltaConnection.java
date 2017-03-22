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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.FileOps;
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
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Test a client connection over a link */  
public abstract class AbstractTestDeltaConnection {

    @BeforeClass public static void setupZone() { 
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
        return create(dsg, 0);
    }
    
    protected static final String DS_NAME = "dsTest";
    protected static final String DS_URI = "http://example/"+DS_NAME;

    protected DeltaConnection create(DatasetGraph shadow, int localVersion) {
        DeltaLink dLink = getLink();
        if ( ! dLink.isRegistered() ) {
            Id clientId = Id.create();
            RegToken regToken = dLink.register(clientId);
        }                
        Id clientId = dLink.getClientId();
        DeltaConnection dConn = DeltaConnection.create(getZone(), clientId, DS_NAME, DS_URI, shadow, dLink);
        return dConn;
    }
    
    protected DeltaConnection reconnect(Id dsRef, DatasetGraph shadow) {
        return DeltaConnection.connect(getZone(), getLink().getClientId(), dsRef, shadow, getLink());
    }
    
    @Test(expected=DeltaBadRequestException.class)
    public void connect_non_existing() {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        DatasetGraph shadow = null;
        Id dsRef = Id.create();
        DeltaConnection dConn = DeltaConnection.connect(getZone(), clientId, dsRef, shadow, dLink);
    }
    
    @Test
    public void create_0() {
        // Create on the Delta link then connect DeltaConnection.
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        String DS_NAME = "123";
        Id dsRef = dLink.newDataSource(DS_NAME, "http://example/datasource");
        DeltaConnection dConn = DeltaConnection.connect(getZone(), clientId, dsRef, /*shadow*/null, dLink);
    }

    @Test
    public void create_1() {
        // Create via DeltaConnection.
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        Id dsRef = Id.create();
        DeltaConnection dConn = DeltaConnection.create(getZone(), clientId, "NEW", /*shadow*/null, /*uri*/null, dLink);
    }

    @Test
    public void create_2() {
        // Create twice
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        DatasetGraph shadow = null;
        
        DeltaConnection dConn1 = DeltaConnection.create(getZone(), clientId, "NEW", /*shadow*/null, /*uri*/null, dLink);
        try {
            DeltaConnection dConn2 = DeltaConnection.create(getZone(), clientId, "NEW", /*shadow*/null, /*uri*/null, dLink);
            fail("Didn't get a DeltaBadRequestException");
        } catch (DeltaBadRequestException ex) {}
    }

    @Test
    public void create_3() {
        // Create twice
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        DeltaConnection dConn = DeltaConnection.create(getZone(), clientId, "NEW", /*shadow*/null, /*uri*/null, dLink);
        String url1 = dConn.getInitialStateURL();
        assertNotNull(url1);
        RDFDataMgr.parse(StreamRDFLib.sinkNull(), url1);
    }

    // Make a change, ensure the local dataset is changed. 
    @Test
    public void change_01() {
        try(DeltaConnection dConn = create()) {
            long verLocal0 = dConn.getLocalVersionNumber();
            long verRemotel0 = dConn.getRemoteVersionLatest();
            
            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                dsg.add(SSE.parseQuad("(:gx :sx :px :ox)"));
            });
            
            int verLocal1 = dConn.getLocalVersionNumber();
            int verRemotel1 = dConn.getRemoteVersionLatest();
            assertEquals(verLocal1, dConn.getLocalVersionNumber());
            assertEquals(verRemotel1, dConn.getRemoteVersionLatest());
            
            assertFalse(dConn.getDatasetGraph().isEmpty());
            if ( dConn.getStorage() != null )
                assertFalse(dConn.getStorage().isEmpty());
        }
    }

    //Make a change, get the patch, apply to a clean dsg. Are the datasets the same?
    @Test
    public void change_02() {
        try(DeltaConnection dConn = create()) {
            Id dsRef = dConn.getDatasourceId();
            int version = dConn.getRemoteVersionLatest();

            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s1 :p1 :o1)");
                dsg.add(q);
            });

            DatasetGraph dsg2 = DatasetGraphFactory.createTxnMem();
            int ver = dConn.getRemoteVersionLatest();
            RDFPatch patch1 = dConn.getLink().fetch(dsRef, ver) ;
            
            
            
            RDFPatchOps.applyChange(dsg2, patch1);

            Set<Quad> set1 = Txn.calculateRead(dsg, ()->Iter.toSet(dsg.find()));
            Set<Quad> set2 = Txn.calculateRead(dsg2, ()->Iter.toSet(dsg2.find()));
            assertEquals(set1, set2);
        }
    }
    
    private void changeTest(Runnable betweenSections) {
        // Make change.
        // Reconnect to the same server and see if the versions reflect the change.
        Quad quad = DeltaTestLib.freshQuad();
        int verLocal = -999;
        int verRemote = -999;
        Id dsRef;
        String name;
        DatasetGraph dsg0;
        
        try(DeltaConnection dConn = create()) {
            dsRef = dConn.getDatasourceId();
            dsg0 = dConn.getStorage();
            DatasetGraph dsg = dConn.getDatasetGraph();
            int ver = dConn.getLocalVersionNumber();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(0, ver);
            // Make change.
            Txn.executeWrite(dsg, ()->dsg.add(quad));
            verLocal = dConn.getLocalVersionNumber();
            assertEquals(ver+1, verLocal);
        }
        
        betweenSections.run();
        
        // Reconnect
        try(DeltaConnection dConn = reconnect(dsRef, dsg0)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            int ver = dConn.getLocalVersionNumber();
            int ver2 = dConn.getRemoteVersionLatest();

            assertEquals(verLocal, ver);
            assertEquals(verLocal, ver2);
            boolean b = Txn.calculateRead(dsg, ()->dsg.contains(quad));
            assertTrue(b);
        }

    }
    
    @Test public void change_03() {
        changeTest(()->{});
    }
    
    @Test public void change_04() {
        changeTest(()->getSetup().relink());
    }

    @Test public void change_05() {
        changeTest(()->getSetup().restart());
    }

    private static boolean equals(RDFPatch patch1, RDFPatch patch2) {
        RDFChangesCollector c1 = new RDFChangesCollector();
        // The getRDFPatch is a RDFPatchStored which supports hashCode and equals.
        RDFChangesCollector.RDFPatchStored p1 = (RDFChangesCollector.RDFPatchStored)c1.getRDFPatch();
        
        RDFChangesCollector c2 = new RDFChangesCollector();
        RDFChangesCollector.RDFPatchStored p2 = (RDFChangesCollector.RDFPatchStored)c2.getRDFPatch();
        
        return Objects.equal(p1, p2);
    }
}
