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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.util.UUID;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.ext.com.google.common.base.Objects;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Tests for the link (multiplex connection to the server or local engine) */
public abstract class AbstractTestDeltaLink {
    @BeforeClass public static void setForTesting() { 
        //LogCtl.setLog4j();
        LogCtl.setJavaLogging();
    }
    
    public abstract Setup.LinkSetup getSetup();
    public DeltaLink getLink() { return getSetup().getLink(); }

    protected static UUID uuid1 = UUID.randomUUID();
    protected static Id id1 = Id.fromUUID(uuid1);
    protected static UUID uuid2 = UUID.randomUUID();
    protected static Id id2 = Id.fromUUID(uuid2);
    
    @Test
    public void register_01() {
        DeltaLink dLink = getLink();
        RegToken regToken = dLink.register(id2);
        assertNotNull(regToken);
    }

    @Test
    public void register_02() { 
        DeltaLink dLink = getLink();
        assertFalse(dLink.isRegistered());
        RegToken regToken = dLink.register(id1);
        assertEquals(id1, dLink.getClientId());
        assertEquals(regToken, dLink.getRegToken());
        assertTrue(dLink.isRegistered());
    }

    // Patch at the link level. 
    @Test
    public void patch_01() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_01", "http://example/");
        
        InputStream in = IO.openFile(DeltaTestLib.DIR+"/patch1.rdfp");
        RDFPatch patch = RDFPatchOps.read(in);

        int version = dLink.getCurrentVersion(dsRef);
        int version1 = dLink.sendPatch(dsRef, patch);
        assertNotEquals(version, version1);

        int version2 = dLink.getCurrentVersion(dsRef);
        assertEquals(version1, version2);
        
        RDFPatch patch1 = dLink.fetch(dsRef, version1) ;
        assertTrue(equals(patch1, patch));
        RDFPatch patch2 = dLink.fetch(dsRef, Id.fromNode(patch.getId())) ;
        assertTrue(equals(patch1, patch2));
    }

    
    
    private static boolean equals(RDFPatch patch1, RDFPatch patch2) {
        RDFChangesCollector c1 = new RDFChangesCollector();
        // The getRDFPatch is a RDFPatchStored which supports hashCode and equals.
        RDFChangesCollector.RDFPatchStored p1 = (RDFChangesCollector.RDFPatchStored)c1.getRDFPatch();
        
        RDFChangesCollector c2 = new RDFChangesCollector();
        RDFChangesCollector.RDFPatchStored p2 = (RDFChangesCollector.RDFPatchStored)c2.getRDFPatch();
        
        return Objects.equal(p1, p2);
    }
    
    // Link test, connection test.
    
    @Test
    public void datasource_01() {
        DeltaLink dLink = getLink();
        assertTrue(dLink.listDatasets().isEmpty());
        Id dsRef = dLink.newDataSource("datasource_01", "http://example/uri");
        assertFalse(dLink.listDatasets().isEmpty());
    }
        
    @Test
    public void datasource_01a() {
        DeltaLink dLink = getLink();
        assertTrue(dLink.listDatasets().isEmpty());
        Id dsRef = dLink.newDataSource("datasource_01", "http://example/uri");

        assertEquals(1, dLink.listDatasets().size());
        assertEquals(dsRef, dLink.listDatasets().get(0));
        
        int version = dLink.getCurrentVersion(dsRef);
        assertEquals(0, version);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNotNull(dsd);
        assertEquals("http://example/uri", dsd.uri);
        assertEquals(dsRef, dsd.id);
        dsd.asJson();
    }

    @Test(expected=DeltaException.class)
    public void datasource_02() {
        DeltaLink dLink = getLink();
        assertEquals(0, dLink.listDatasets().size());
        Id dsRef1 = dLink.newDataSource("datasource_01", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        
        // Already exists : URI is not a factor.
        Id dsRef2 = dLink.newDataSource("datasource_01", "http://example/uri2");
    }
        
    @Test
    public void datasource_03() {
        DeltaLink dLink = getLink();
        assertEquals(0, dLink.listDatasets().size());
        Id dsRef1 = dLink.newDataSource("datasource_01", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        // Does not exist : URI is not a factor.
        Id dsRef2 = dLink.newDataSource("datasource_02", "http://example/uri");
        assertNotEquals(dsRef1, dsRef2);
        assertEquals(2, dLink.listDatasets().size());
    }

    //    // -> connection
//    @Test
//    public void connection_01() {
//        DeltaLink dLink = getLink();
//        Id dsRef = dLink.newDataSource("datasource_01", "http://example/uri");
//
//        Id clientId = Id.create();
//        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
//        
//        DeltaConnection dConn = DeltaConnection.create("label",
//                                                       clientId, dsRef,
//                                                       dsg,
//                                                       dLink);
//        int v = dConn.getLocalVersionNumber();
//        assertNotNull(dConn.getName());
//        assertEquals(0, dConn.getLocalVersionNumber());
//        assertEquals(0, dConn.getRemoteVersionNumber());
//        assertEquals(0, dConn.getRemoteVersionLatest());
//        assertNotNull(dConn.getStorage());
//
//        // Check the DSG  
//        assertEquals(dsg, dConn.getStorage());
//        DatasetGraph dsg1 = dConn.getDatasetGraph();
//        assertNotEquals(dsg, dsg1);
//        assertTrue(dsg1 instanceof DatasetGraphChanges);
//    }
}
