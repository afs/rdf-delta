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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.util.List ;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.ext.com.google.common.base.Objects;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.system.StreamRDFLib ;
import org.junit.BeforeClass ;
import org.junit.Test;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Tests for the link (multiplex connection to the server or local engine) */
public abstract class AbstractTestDeltaLink {
    @BeforeClass public static void setForTesting() { 
        LogCtl.setLog4j();
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }

    protected static final String FILES_DIR = DeltaTestLib.TDIR+"test_dlink/";
    
    public abstract Setup.LinkSetup getSetup();
    public DeltaLink getLink() { return getSetup().getLink(); }

    public DeltaLink getLinkRegistered() { 
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        return dLink;
    }

    @Test
    public void ping_01() {
        DeltaLink dLink = getLink();
        dLink.ping();
    }
    
    @Test
    public void register_01() {
        DeltaLink dLink = getLink();
        Id id = Id.create();
        RegToken regToken = dLink.register(id);
        assertNotNull(regToken);
    }

    @Test
    public void register_02() { 
        DeltaLink dLink = getLink();
        Id id = Id.create();
        assertFalse(dLink.isRegistered());
        RegToken regToken = dLink.register(id);
        assertEquals(id, dLink.getClientId());
        assertEquals(regToken, dLink.getRegToken());
        assertTrue(dLink.isRegistered());
    }

    @Test
    public void register_03() { 
        DeltaLink dLink = getLink();
        Id id = Id.create();
        assertFalse(dLink.isRegistered());
        RegToken regToken = dLink.register(id);
        assertTrue(dLink.isRegistered());
        dLink.deregister();
        assertFalse(dLink.isRegistered());
        assertNull(dLink.getRegToken());
        // Remember last clientId
        assertNotNull(dLink.getClientId());
    }

    @Test
    public void register_04() { 
        DeltaLink dLink = getLink();
        assertFalse(dLink.isRegistered());
        // Bad
    }

    @Test
    public void register_05() { 
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken1 = dLink.register(clientId);
        RegToken regToken2 = dLink.register(clientId);
        dLink.deregister();
        assertFalse(dLink.isRegistered());
    }

    @Test
    public void register_06() { 
        // Not valid for DeltaLinkLocal.  Multiple registration is not provided. 
        // Valid for for DeltaLinkHTTP and happens when a client restarts.
        DeltaLink dLink = getLink();
        Id clientId1 = Id.create();
        Id clientId2 = Id.create();
        assertNotEquals(clientId1, clientId2);
        RegToken regToken1 = dLink.register(clientId1);
        RegToken regToken2 = dLink.register(clientId2);
        // New registration token.
        assertNotEquals(regToken1, regToken2);
    }

    @Test
    public void register_07() {
        // Two separate registrations.
        DeltaLink dLink1 = getSetup().createLink();
        DeltaLink dLink2 = getSetup().createLink();
        Id clientId1 = Id.create();
        Id clientId2 = Id.create();
        assertNotEquals(clientId1, clientId2);
        RegToken regToken1 = dLink1.register(clientId1);
        RegToken regToken2 = dLink2.register(clientId2);
        assertNotEquals(regToken1, regToken2);
    }
    
    //---- Patches that are bad in some way.
    
    @Test(expected=DeltaException.class)
    public void patch_bad_01()  { patch_bad("patch_bad_1.rdfp"); }
    
    @Test(expected=DeltaException.class)
    public void patch_bad_02()  { patch_bad("patch_bad_2.rdfp"); }
    
    @Test(expected=DeltaException.class)
    public void patch_bad_03()  { patch_bad("patch_bad_3.rdfp"); }
    
    private void patch_bad(String filename) {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource(filename, "http://example/");
        RDFPatch patch = RDFPatchOps.read(FILES_DIR+filename);
        int version = dLink.append(dsRef, patch);
        fail("Should not get here");
    }

    // Patch at the link level. 
    @Test
    public void patch_01() {
        // LOG OUPUT
        
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_01", "http://example/");
        
        InputStream in = IO.openFile(FILES_DIR+"/patch1.rdfp");
        RDFPatch patch = RDFPatchOps.read(in);

        int version = dLink.getCurrentVersion(dsRef); // 0??
        int version1 = dLink.append(dsRef, patch); // Should be 0 ***********
        assertNotEquals(version, version1);

        int version2 = dLink.getCurrentVersion(dsRef);
        assertEquals(version1, version2);
        
        RDFPatch patch1 = dLink.fetch(dsRef, version1) ;
        assertNotNull(patch1);
//        if ( ! equals(patch1, patch) ) {
//            System.out.println("**** Patch (as read)");
//            RDFPatchOps.write(System.out, patch);
//            System.out.println("**** Patch (as fetched)");
//            RDFPatchOps.write(System.out, patch);
//            equals(patch1, patch);
//        }
        
        assertTrue(equals(patch1, patch));
        RDFPatch patch2 = dLink.fetch(dsRef, Id.fromNode(patch.getId())) ;
        assertNotNull(patch2);
        assertTrue(equals(patch1, patch2));
    }

    @Test
    public void patch_02() {
        // Unregistered patch
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_02", "http://example/");
        dLink.deregister();
        try { 
            RDFPatch patch = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
            int version1 = dLink.append(dsRef, patch);
            fail("Managed to send a patch when not registered");
        } catch (DeltaException ex) {} 
    }
    
    @Test
    public void patch_03() {
        // patch1 then patch2
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_03", "http://example/");

        RDFPatch patch1 = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        RDFPatch patch2 = RDFPatchOps.read(FILES_DIR+"/patch2.rdfp");

        int version1 = dLink.append(dsRef, patch1);
        assertEquals(1, version1);
        PatchLogInfo logInfo1 = dLink.getPatchLogInfo(dsRef);
        assertEquals(1, logInfo1.maxVersion);
        
        int version2 = dLink.append(dsRef, patch2);
        assertEquals(2, version2);
        PatchLogInfo logInfo2 = dLink.getPatchLogInfo(dsRef);
        assertEquals(2, logInfo2.maxVersion);
        assertEquals(1, logInfo2.minVersion);
    }
    
    static int counter = 1 ;
    private void patch_seq(String...filenames) {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_seq_"+(counter++), "http://example/");
        for ( String fn : filenames ) {
            RDFPatch patch = RDFPatchOps.read(FILES_DIR+fn);
            dLink.append(dsRef, patch);
        }
    }
    
    public void patch_seq_01() {
        patch_seq("patch1.rdfp", "patch2.rdfp", "patch3.rdfp");
    }
    
    @Test(expected=DeltaException.class)
    public void patch_seq_bad_02() {
        // patch1 then patch1 again -> error.
        patch_seq("patch1.rdfp", "patch1.rdfp");
        fail("Should not get here");
    }
    
    @Test(expected=DeltaException.class)
    public void patch_seq_bad_03() {
        // patch1 then patch3 (non-existent previous)
        patch_seq("patch1.rdfp", "patch3.rdfp");
        fail("Should not get here");
    }
    
    @Test(expected=DeltaException.class)
    public void patch_seq_bad_04() {
        // patch1 then patch2 then patch2 again
        patch_seq("patch1.rdfp", "patch2.rdfp", "patch2.rdfp");
        fail("Should not get here");
    }

    // Link test, connection test.
    
    @Test
    public void datasource_01() {
        DeltaLink dLink = getLink();
        assertTrue(dLink.listDatasets().isEmpty());
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        Id dsRef = dLink.newDataSource("datasource_02", "http://example/uri");
        assertFalse(dLink.listDatasets().isEmpty());
    }
        
    @Test
    public void datasource_02() {
        DeltaLink dLink = getLinkRegistered();

        assertTrue(dLink.listDatasets().isEmpty());
        assertTrue(dLink.allDescriptions().isEmpty());

        Id dsRef = dLink.newDataSource("datasource_01", "http://example/uri");

        assertEquals(1, dLink.listDatasets().size());
        assertEquals(dsRef, dLink.listDatasets().get(0));
        
        int version = dLink.getCurrentVersion(dsRef);
        assertEquals(0, version);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNotNull(dsd);
        assertEquals("http://example/uri", dsd.uri);
        assertEquals(dsRef, dsd.id);
        // Ensure this works.
        dsd.asJson();
    }

    @Test
    public void datasource_02a() {
        // As 02 but by URI.
        DeltaLink dLink = getLinkRegistered();
        String uri = "http://example/uri2a";
        Id dsRef = dLink.newDataSource("datasource_02a", uri);

        assertEquals(1, dLink.listDatasets().size());
        assertEquals(dsRef, dLink.listDatasets().get(0));
        
        DataSourceDescription dsd = dLink.getDataSourceDescription(uri);
        assertNotNull(dsd);
        assertEquals(uri, dsd.uri);
        assertEquals(dsRef, dsd.id);
    }

    @Test(expected=DeltaException.class)
    public void datasource_03() {
        DeltaLink dLink = getLink();
        assertEquals(0, dLink.listDatasets().size());
        assertEquals(0, dLink.allDescriptions().size());

        // Not registered.
        Id dsRef1 = dLink.newDataSource("datasource_03", "http://example/uri");
    }

    @Test
    public void datasource_04() {
        DeltaLink dLink = getLinkRegistered();
        assertEquals(0, dLink.listDatasets().size());
        Id dsRef1 = dLink.newDataSource("datasource_04", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        assertEquals(1, dLink.allDescriptions().size());

        try {
            // Check where the exception occurs.
            // Already exists : URI is not a factor.
            Id dsRef2 = dLink.newDataSource("datasource_04", "http://example/uri2");
            fail("Managed to create twice");
        } catch (DeltaException ex) {}
    }

    @Test
    public void datasource_05() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef1 = dLink.newDataSource("datasource_05a", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        // Does not exist : URI is not a factor.
        Id dsRef2 = dLink.newDataSource("datasource_05b", "http://example/uri");
        assertNotEquals(dsRef1, dsRef2);
        assertEquals(2, dLink.listDatasets().size());
    }
    
    @Test
    public void datasource_10() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        dLink.removeDataset(dsRef);
        assertEquals(0, dLink.listDatasets().size());
    }

    @Test
    public void datasource_11() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        dLink.removeDataset(dsRef);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNull(dsd);
    }

    @Test
    public void datasource_12() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        dLink.removeDataset(dsRef);
        assertEquals(0, dLink.listDatasets().size());
        // Again.
        dLink.removeDataset(dsRef);
    }
    
    @Test
    public void datasource_13_not_found() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        Id dsRef1 = Id.create();
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef1);
        assertNull(dsd);
    }
    
    @Test
    public void datasource_14_not_found() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        DataSourceDescription dsd = dLink.getDataSourceDescription("http://example/uri-not-present");
        assertNull(dsd);
    }

    @Test
    public void datasource_15_init() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_15", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        DataSourceDescription dsd = dLink.getDataSourceDescription("http://example/uri-not-present");
        String url = dLink.initialState(dsRef);
        assertNotNull(url);
        RDFDataMgr.parse(StreamRDFLib.sinkNull(), url);
    }

    @Test
    public void datasource_16_listDescr() {
        DeltaLink dLink = getLinkRegistered();
        List<DataSourceDescription> all = dLink.allDescriptions();
        assertEquals(0, all.size());
        Id dsRef = dLink.newDataSource("datasource_16", "http://example/uri16");
        assertEquals(1, dLink.listDatasets().size());
        all = dLink.allDescriptions();
        assertEquals(1, all.size());
        boolean b = all.stream().anyMatch(dsd->dsd.getUri().equals("http://example/uri16"));
        assertTrue(b);
    }

    @Test
    public void datasource_17_listLog() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_17", "http://example/uri17");
        List<Id> x = dLink.listDatasets();
        assertEquals(1, x.size());
        assertTrue(x.contains(dsRef));
        
        PatchLogInfo logInfo = dLink.getPatchLogInfo(dsRef);
        assertEquals(dsRef, logInfo.dataSourceId);
    }

    private static boolean equals(RDFPatch patch1, RDFPatch patch2) {
        RDFChangesCollector c1 = new RDFChangesCollector();
        patch1.apply(c1);
        // The getRDFPatch is a RDFPatchStored which supports hashCode and equals.
        RDFChangesCollector.RDFPatchStored p1 = (RDFChangesCollector.RDFPatchStored)c1.getRDFPatch();
        
        RDFChangesCollector c2 = new RDFChangesCollector();
        patch2.apply(c2);
        RDFChangesCollector.RDFPatchStored p2 = (RDFChangesCollector.RDFPatchStored)c2.getRDFPatch();
        
        return Objects.equal(p1, p2);
    }
}
