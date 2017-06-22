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
        long version = dLink.append(dsRef, patch);
        fail("Should not get here");
    }

    // Patch at the link level. 
    @Test
    public void patch_add_1() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_01", "http://example/");
        
        InputStream in = IO.openFile(FILES_DIR+"/patch1.rdfp");
        RDFPatch patch = RDFPatchOps.read(in);

        long version = dLink.getCurrentVersion(dsRef); // 0
        long version1 = dLink.append(dsRef, patch);    // Should be 1
        assertNotEquals(version, version1);

        long version2 = dLink.getCurrentVersion(dsRef);
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
    public void patch_add_error_1() {
        // Unregistered patch
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_02", "http://example/");
        dLink.deregister();
        try { 
            RDFPatch patch = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
            long version1 = dLink.append(dsRef, patch);
            fail("Managed to send a patch when not registered");
        } catch (DeltaException ex) {} 
    }
    
    @Test
    public void patch_add_add() {
        // patch1 then patch2,checkign the versions advance as expected.
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_03", "http://example/");

        PatchLogInfo logInfo0 = dLink.getPatchLogInfo(dsRef);
        assertEquals(0, logInfo0.getMaxVersion());
        assertEquals(0, logInfo0.getMinVersion());

        RDFPatch patch1 = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        RDFPatch patch2 = RDFPatchOps.read(FILES_DIR+"/patch2.rdfp");

        long version1 = dLink.append(dsRef, patch1);
        assertEquals(1, version1);
        
        PatchLogInfo logInfo1 = dLink.getPatchLogInfo(dsRef);
        assertEquals(1, logInfo1.getMaxVersion());
        assertEquals(1, logInfo1.getMinVersion());
        
        long version2 = dLink.append(dsRef, patch2);
        assertEquals(2, version2);
        PatchLogInfo logInfo2 = dLink.getPatchLogInfo(dsRef);
        assertEquals(2, logInfo2.getMaxVersion());
        assertEquals(1, logInfo2.getMinVersion());
    }
    
    @Test
    public void patch_http404_01() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_04", "http://example/");
        RDFPatch patch = dLink.fetch(dsRef, 99);
        assertNull(patch);
    }
    
    @Test
    public void patch_http404_02() {
        // Patches start at 1.
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_04", "http://example/");
        RDFPatch patch = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        long version1 = dLink.append(dsRef, patch);
        
        RDFPatch patch0 = dLink.fetch(dsRef, 0);
        assertNull(patch0);
        RDFPatch patch1 = dLink.fetch(dsRef, 1);
        assertNotNull(patch1);
    }
    
    static int counter = 1 ;
    private void patch_seq(String...filenames) {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("patch_seq_"+(counter++), "http://example/");
        patch_send(dsRef, filenames);
    }
    
    private void patch_send(Id dsRef, String...filenames) {
        DeltaLink dLink = getLinkRegistered();
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
    public void datasource_create_01() {
        DeltaLink dLink = getLink();
        assertTrue(dLink.listDatasets().isEmpty());
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        Id dsRef = dLink.newDataSource("datasource_create_01", "http://example/uri");
        assertFalse(dLink.listDatasets().isEmpty());
        assertEquals(1, dLink.listDatasets().size());
        
        long version = dLink.getCurrentVersion(dsRef);
        PatchLogInfo info = dLink.getPatchLogInfo(dsRef);

        assertEquals(0, version);
        assertEquals(0, info.getMinVersion());
        assertEquals(0, info.getMaxVersion());
    }
        
    @Test
    public void datasource_create_02() {
        DeltaLink dLink = getLinkRegistered();

        assertTrue(dLink.listDatasets().isEmpty());
        assertTrue(dLink.listDescriptions().isEmpty());

        Id dsRef = dLink.newDataSource("datasource_create_02", "http://example/uri");

        assertEquals(1, dLink.listDatasets().size());
        assertEquals(dsRef, dLink.listDatasets().get(0));
        
        long version = dLink.getCurrentVersion(dsRef);
        assertEquals(0, version);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNotNull(dsd);
        assertEquals("http://example/uri", dsd.getUri());
        assertEquals(dsRef, dsd.getId());
        // Ensure this works.
        dsd.asJson();
    }

    @Test
    public void datasource_create_03() {
        // As 02 but by URI.
        DeltaLink dLink = getLinkRegistered();
        String uri = "http://example/uri2a";
        Id dsRef = dLink.newDataSource("datasource_create_03", uri);

        assertEquals(1, dLink.listDatasets().size());
        assertEquals(1, dLink.listDescriptions().size());

        assertEquals(dsRef, dLink.listDatasets().get(0));
        assertEquals(dsRef, dLink.listDescriptions().stream().findFirst().get().getId());
        
        DataSourceDescription dsd = dLink.getDataSourceDescription(uri);
        assertNotNull(dsd);
        assertEquals(uri, dsd.getUri());
        assertEquals(dsRef, dsd.getId());
    }

    @Test
    public void datasource_create_04() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef1 = dLink.newDataSource("datasource_create_04_a", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        // Does not exist : new name : URI is not a factor.
        Id dsRef2 = dLink.newDataSource("datasource_create_04_b", "http://example/uri");
        assertNotEquals(dsRef1, dsRef2);
        assertEquals(2, dLink.listDatasets().size());
    }
    
    @Test
    public void datasource_create_05() {
        DeltaLink dLink = getLinkRegistered();
        assertEquals(0, dLink.listDatasets().size());
        Id dsRef1 = dLink.newDataSource("datasource_create_05", "http://example/uri");
    
        try {
            // Check where the exception occurs.
            // Name already exists : URI is not a factor.
            Id dsRef2 = dLink.newDataSource("datasource_create_05", "http://example/uri2");
            fail("Managed to create twice");
        } catch (DeltaException ex) {}
    }
    
    @Test
    public void datasource_list_01() {
        DeltaLink dLink = getLink();
        assertEquals(0, dLink.listDatasets().size());
        assertEquals(0, dLink.listDescriptions().size());
    }

    @Test
    public void datasource_list_01a() {
        DeltaLink dLink = getLinkRegistered();
        assertEquals(0, dLink.listDatasets().size());
        assertEquals(0, dLink.listDescriptions().size());
    }

    @Test
    public void datasource_list_02() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_list_2", "http://example/uri");
        assertNotNull(dsRef);
        assertNotEquals(dsRef, Id.nullId());
        assertEquals(1, dLink.listDatasets().size());
        assertEquals(1, dLink.listDescriptions().size());
    }

    @Test
    public void datasource_listDSD_03() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_listDSD_03", "http://example/uri16");
        assertEquals(1, dLink.listDatasets().size());
        List<DataSourceDescription> all = dLink.listDescriptions();
        assertEquals(1, all.size());
        boolean b = all.stream().anyMatch(dsd->dsd.getUri().equals("http://example/uri16"));
        assertTrue(b);
    }
    @Test
    public void datasource_listLog_04() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_listLog_04", "http://example/uri17");
        List<Id> x = dLink.listDatasets();
        assertEquals(1, x.size());
        assertTrue(x.contains(dsRef));
        
        PatchLogInfo logInfo = dLink.getPatchLogInfo(dsRef);
        assertEquals(dsRef, logInfo.getDataSourceId());
    }
    
    @Test
    public void datasource_remove_01() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_remove_01", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        dLink.removeDataSource(dsRef);
        assertEquals(0, dLink.listDatasets().size());
    }

    @Test
    public void datasource_remove_02() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_remote_02", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        dLink.removeDataSource(dsRef);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNull(dsd);
    }

    @Test
    public void datasource_remove_03() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        dLink.removeDataSource(dsRef);
        assertEquals(0, dLink.listDatasets().size());
        // Again.
        dLink.removeDataSource(dsRef);
    }
    
    @Test
    public void datasource_not_found_01() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        Id dsRef1 = Id.create();
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef1);
        assertNull(dsd);
    }
    
    @Test
    public void datasource_not_found_02() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        DataSourceDescription dsd = dLink.getDataSourceDescription("http://example/uri-not-present");
        assertNull(dsd);
    }

    @Test
    public void datasource_init_01() {
        DeltaLink dLink = getLinkRegistered();
        Id dsRef = dLink.newDataSource("datasource_15", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        DataSourceDescription dsd = dLink.getDataSourceDescription("http://example/uri-not-present");
        String url = dLink.initialState(dsRef);
        assertNotNull(url);
        RDFDataMgr.parse(StreamRDFLib.sinkNull(), url);
    }

    @Test
    public void create_delete_dLink() {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);

        Id dsRef = dLink.newDataSource("create_delete_01", "http://example/cd1");
        dLink.removeDataSource(dsRef);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNull(dsd);
    }
    
    @Test
    public void create_delete_create_dLink_1() {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);

        Id dsRef = dLink.newDataSource("create_delete_create_1", "http://example/cdc");
        // Options.
        dLink.removeDataSource(dsRef);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNull(dsd);

        // Need to remove from disk for this to pass.
        // Markign "disabled" in-place will fail. 
        
        Id dsRef2 = dLink.newDataSource("create_delete_create_1", "http://example/cdc");
        assertNotEquals(dsRef,  dsRef2);
    }

    @Test
    public void create_delete_create_dLink_2() {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        Id dsRef = dLink.newDataSource("create_delete_create_2", "http://example/cdc");

        // Add a patch
        patch_send(dsRef, "patch-empty.rdfp");
        
        // Delete
        dLink.removeDataSource(dsRef);
        // remake
        Id dsRef2 = dLink.newDataSource("create_delete_create_2", "http://example/cdc");
        assertNotEquals(dsRef,  dsRef2);
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
