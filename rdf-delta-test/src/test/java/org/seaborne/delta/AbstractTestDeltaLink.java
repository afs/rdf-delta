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

import static org.junit.Assert.*;

import java.util.List ;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.ext.com.google.common.base.Objects;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphListener;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.system.StreamRDFLib ;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.util.graph.GraphListenerBase;
import org.apache.jena.web.HttpSC;
import org.junit.BeforeClass ;
import org.junit.Test;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLinkCounter;
import org.seaborne.delta.link.DeltaLinkEvents;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.PatchSummary;
import org.seaborne.patch.changes.RDFChangesCollector;
import org.seaborne.patch.changes.RDFChangesCounter;

/** Tests for the link (multiplex connection to the server or local engine) */
public abstract class AbstractTestDeltaLink {
    private static Version version_1 = Version.create(1);
    private static Version version_2 = Version.create(2);
    private static Version version_3 = Version.create(3);
    private static Version version_4 = Version.create(4);

    @BeforeClass public static void setForTesting() {
        LogCtl.setJavaLogging("src/test/resources/logging.properties");
    }

    protected static final String FILES_DIR = DeltaTestLib.TDIR+"test_dlink/";

    public abstract Setup.LinkSetup getSetup();
    public DeltaLink getLink() { return getSetup().getLink(); }

    @Test
    public void ping_01() {
        DeltaLink dLink = getLink();
        dLink.ping();
    }

    //---- Patches that are bad in some way.

    @Test(expected=DeltaException.class)
    public void patch_bad_01()  { patch_bad("patch_bad_1.rdfp"); }

    @Test(expected=DeltaException.class)
    public void patch_bad_02()  { patch_bad("patch_bad_2.rdfp"); }

    @Test(expected=DeltaException.class)
    public void patch_bad_03()  { patch_bad("patch_bad_3.rdfp"); }

    private void patch_bad(String filename) {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource(filename, "http://example/");
        RDFPatch patch = RDFPatchOps.read(FILES_DIR+filename);
        Version version = dLink.append(dsRef, patch);
        fail("Should not get here");
    }

    // Patch at the link level.
    @Test
    public void patch_append_1() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_append_1", "http://example/");

        RDFPatch patch = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");

        Version version = dLink.getCurrentVersion(dsRef); // 0
        Version version1 = dLink.append(dsRef, patch);    // Should be 1
        assertNotEquals(version, version1);
        assertEquals(version_1, version1);

        Version version2 = dLink.getCurrentVersion(dsRef);
        assertEquals(version1, version2);

        RDFPatch patch1 = dLink.fetch(dsRef, version1) ;
        assertNotNull(patch1);
        assertTrue(equals(patch1, patch));
        RDFPatch patch2 = dLink.fetch(dsRef, Id.fromNode(patch.getId())) ;
        assertNotNull(patch2);
        assertTrue(equals(patch1, patch2));
    }

    @Test
    public void patch_append_2() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_append_2", "http://example/");

        RDFPatch patch1 = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        RDFPatch patch2 = RDFPatchOps.read(FILES_DIR+"/patch2.rdfp");
        assertEquals(patch1.getId(), patch2.getPrevious());

        Version version = dLink.getCurrentVersion(dsRef); // 0
        Version version1 = dLink.append(dsRef, patch1);    // Should be 1
        assertEquals(version_1, version1);

        // Send second patch.
        Version version2 = dLink.append(dsRef, patch2);
        assertEquals(version_2, version2);
    }

    @Test
    public void patch_append_3() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_append_2", "http://example/");

        RDFPatch patch1 = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        RDFPatch patch2 = RDFPatchOps.read(FILES_DIR+"/patch2.rdfp");
        assertEquals(patch1.getId(), patch2.getPrevious());

        Version version = dLink.getCurrentVersion(dsRef); // 0
        Version version1 = dLink.append(dsRef, patch1);    // Should be 1
        assertEquals(version_1, version1);

        // Send again.  Resend the head is acceptable.
        Version version1a = dLink.append(dsRef, patch1);
        assertEquals(version1, version1a);

        Version version2 = dLink.append(dsRef, patch2);
        assertEquals(version_2, version2);

        Version version2a = dLink.append(dsRef, patch2);
        assertEquals(version2, version2a);

        // Can't send patch1 now.
        try {
            Version version1b = dLink.append(dsRef, patch1);
            fail("Managed to resend earlier patch");
            //assertEquals(-1, version1b);
        } catch (DeltaBadPatchException ex) {
            assertEquals(HttpSC.BAD_REQUEST_400, ex.getStatusCode());
        }
    }

    @Test
    public void patch_add_add() {
        // patch1 then patch2, checking the versions advance as expected.
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_add_add", "http://example/");

        PatchLogInfo logInfo0 = dLink.getPatchLogInfo(dsRef);
        assertEquals(Version.INIT, logInfo0.getMaxVersion());
        assertEquals(Version.INIT, logInfo0.getMinVersion());

        RDFPatch patch1 = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        RDFPatch patch2 = RDFPatchOps.read(FILES_DIR+"/patch2.rdfp");

        Version version1 = dLink.append(dsRef, patch1);
        assertEquals(version_1, version1);

        PatchLogInfo logInfo1 = dLink.getPatchLogInfo(dsRef);
        assertEquals(version_1, logInfo1.getMaxVersion());
        assertEquals(version_1, logInfo1.getMinVersion());

        Version version2 = dLink.append(dsRef, patch2);
        assertEquals(version_2, version2);
        PatchLogInfo logInfo2 = dLink.getPatchLogInfo(dsRef);
        assertEquals(version_2, logInfo2.getMaxVersion());
        assertEquals(version_1, logInfo2.getMinVersion());
    }

    @Test//(expected=DeltaNotFoundException.class)
    public void patch_http404_01() {
        // No such patch.
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_404_1", "http://example/");
        RDFPatch patch = dLink.fetch(dsRef, Version.create(99));
        assertNull(patch);
    }

    @Test//(expected=DeltaNotFoundException.class)
    public void patch_http404_02() {
        // Patches start at 1.
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_404_2", "http://example/");
        RDFPatch patch = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        Version version1 = dLink.append(dsRef, patch);

        RDFPatch patch0 = dLink.fetch(dsRef, Version.INIT);
        assertNull(patch0);
        RDFPatch patch1 = dLink.fetch(dsRef, Version.create(1));
        assertNotNull(patch1);
        RDFPatch patch2 = dLink.fetch(dsRef, Version.UNSET);
        assertNull(patch2);
    }

    @Test//(expected=DeltaNotFoundException.class)
    public void patch_http404_03() {
        // Patches start at 1.
        // No such patch is a null return, not an exception.
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_404_2", "http://example/");
        RDFPatch patch = RDFPatchOps.read(FILES_DIR+"/patch1.rdfp");
        Version version1 = dLink.append(dsRef, patch);

        RDFPatch patch0 = dLink.fetch(dsRef, Version.INIT);
        assertNull(patch0);
        RDFPatch patch1 = dLink.fetch(dsRef, Version.create(1));
        assertNotNull(patch1);
    }

    static int counter = 1 ;
    private void patch_seq(String...filenames) {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("patch_seq_"+(counter++), "http://example/");
        patch_send(dsRef, filenames);
    }

    private void patch_send(Id dsRef, String...filenames) {
        DeltaLink dLink = getLink();
        for ( String fn : filenames ) {
            RDFPatch patch = RDFPatchOps.read(FILES_DIR+fn);
            dLink.append(dsRef, patch);
        }
    }

    public void patch_seq_01() {
        patch_seq("patch1.rdfp", "patch2.rdfp", "patch3.rdfp");
    }

    public void patch_seq_02() {
        // patch1 then patch1 again -> Ok.
        patch_seq("patch1.rdfp", "patch1.rdfp");
    }

    @Test(expected=DeltaException.class)
    public void patch_seq_bad_03() {
        // patch1 then patch3 (non-existent previous)
        patch_seq("patch1.rdfp", "patch3.rdfp");
    }

    @Test
    public void patch_seq_bad_04() {
        // patch1 then patch2 then patch2 again -> OK
        patch_seq("patch1.rdfp", "patch2.rdfp", "patch2.rdfp");
    }

    @Test(expected=DeltaException.class)
    public void patch_seq_bad_05() {
        // patch1 then patch2 then patch1 again -> bad.
        patch_seq("patch1.rdfp", "patch2.rdfp", "patch1.rdfp");
    }
    // Link test, connection test.

    @Test
    public void datasource_create_01() {
        DeltaLink dLink = getLink();
        assertTrue(dLink.listDatasets().isEmpty());
        Id dsRef = dLink.newDataSource("datasource_create_01", "http://example/uri");
        assertFalse(dLink.listDatasets().isEmpty());
        assertEquals(1, dLink.listDatasets().size());

        Version version = dLink.getCurrentVersion(dsRef);
        PatchLogInfo info = dLink.getPatchLogInfo(dsRef);

        assertEquals(Version.INIT, version);
        assertEquals(Version.INIT, info.getMinVersion());
        assertEquals(Version.INIT, info.getMaxVersion());
    }

    @Test
    public void datasource_create_02() {
        DeltaLink dLink = getLink();

        assertTrue(dLink.listDatasets().isEmpty());
        assertTrue(dLink.listDescriptions().isEmpty());

        Id dsRef = dLink.newDataSource("datasource_create_02", "http://example/uri");

        assertEquals(1, dLink.listDatasets().size());
        assertEquals(dsRef, dLink.listDatasets().get(0));

        Version version = dLink.getCurrentVersion(dsRef);
        assertEquals(Version.INIT, version);
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
        DeltaLink dLink = getLink();
        String uri = "http://example/uri2a";
        Id dsRef = dLink.newDataSource("datasource_create_03", uri);

        assertEquals(1, dLink.listDatasets().size());
        assertEquals(1, dLink.listDescriptions().size());

        assertEquals(dsRef, dLink.listDatasets().get(0));
        assertEquals(dsRef, dLink.listDescriptions().stream().findFirst().get().getId());

        DataSourceDescription dsd = dLink.getDataSourceDescriptionByURI(uri);
        assertNotNull(dsd);
        assertEquals(uri, dsd.getUri());
        assertEquals(dsRef, dsd.getId());
    }

    @Test
    public void datasource_create_04() {
        DeltaLink dLink = getLink();
        Id dsRef1 = dLink.newDataSource("datasource_create_04_a", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        // Does not exist : new name : URI is not a factor.
        Id dsRef2 = dLink.newDataSource("datasource_create_04_b", "http://example/uri");
        assertNotEquals(dsRef1, dsRef2);
        assertEquals(2, dLink.listDatasets().size());
    }

    @Test
    public void datasource_create_05() {
        DeltaLink dLink = getLink();
        assertTrue(dLink.listDatasets().isEmpty());
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
        assertTrue(dLink.listDatasets().isEmpty());
        assertTrue(dLink.listDescriptions().isEmpty());
        assertTrue(dLink.listPatchLogInfo().isEmpty());
    }

    @Test
    public void datasource_list_02() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_list_2", "http://example/uri");
        assertNotNull(dsRef);
        assertNotEquals(dsRef, Id.nullId());
        assertEquals(1, dLink.listDatasets().size());
        assertEquals(1, dLink.listDescriptions().size());
        assertEquals(1, dLink.listPatchLogInfo().size());
    }

    @Test
    public void datasource_listDSD_03() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_listDSD_03", "http://example/uri16");
        assertEquals(1, dLink.listDatasets().size());
        List<DataSourceDescription> all = dLink.listDescriptions();
        assertEquals(1, all.size());
        boolean b = all.stream().anyMatch(dsd->dsd.getUri().equals("http://example/uri16"));
        assertTrue(b);
    }

    @Test
    public void datasource_listLog_04() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_listLog_04", "http://example/uri17");
        List<PatchLogInfo> x = dLink.listPatchLogInfo();
        assertEquals(1, x.size());
        PatchLogInfo logInfo1 = x.get(0);
        assertNotNull(logInfo1.getDataSourceDescr());
        PatchLogInfo logInfo2 = dLink.getPatchLogInfo(dsRef);
        assertEquals(dsRef, logInfo1.getDataSourceId());
        assertEquals(logInfo1, logInfo2);
    }

    @Test
    public void datasource_listLog_05() {
        DeltaLink dLink = getLink();
        Id dsRef1 = dLink.newDataSource("datasource_listLog_05-1", "http://example/uri18a");
        Id dsRef2 = dLink.newDataSource("datasource_listLog_05-2", "http://example/uri18b");
        List<PatchLogInfo> x = dLink.listPatchLogInfo();
        assertEquals(2, x.size());
        PatchLogInfo logInfo1 = x.get(0);
        PatchLogInfo logInfo2 = x.get(1);
        assertNotEquals(logInfo1, logInfo2);
    }

    @Test
    public void datasource_remove_01() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_remove_01", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        dLink.removeDataSource(dsRef);
        assertTrue(dLink.listDatasets().isEmpty());
    }

    @Test
    public void datasource_remove_02() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_remote_02", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        dLink.removeDataSource(dsRef);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNull(dsd);
    }

    @Test
    public void datasource_remove_03() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        dLink.removeDataSource(dsRef);
        assertTrue(dLink.listDatasets().isEmpty());
        // Again.
        dLink.removeDataSource(dsRef);
    }

    @Test
    public void datasource_not_found_01() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        Id dsRef1 = Id.create();
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef1);
        assertNull(dsd);
    }

    @Test
    public void datasource_not_found_02() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_06", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByURI("http://example/uri-not-present");
        assertNull(dsd);
    }

    //@Test
    // Feature Initial state not active.
    public void datasource_init_01() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("datasource_15", "http://example/uri");
        assertEquals(1, dLink.listDatasets().size());
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByURI("http://example/uri-not-present");
        String url = dLink.initialState(dsRef);
        assertNotNull(url);
        RDFDataMgr.parse(StreamRDFLib.sinkNull(), url);
    }

    @Test
    public void create_delete_dLink() {
        DeltaLink dLink = getLink();

        Id dsRef = dLink.newDataSource("create_delete_01", "http://example/cd1");
        dLink.removeDataSource(dsRef);
        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNull(dsd);
    }

    @Test
    public void create_delete_create_dLink_1() {
        DeltaLink dLink = getLink();

        Id dsRef = dLink.newDataSource("create_delete_create_1", "http://example/cdc");
        dLink.removeDataSource(dsRef);

        DataSourceDescription dsd = dLink.getDataSourceDescription(dsRef);
        assertNull(dsd);

        // Need to remove from disk for this to pass.
        // Marking "disabled" in-place will fail.

        Id dsRef2 = dLink.newDataSource("create_delete_create_1", "http://example/cdc");
        assertNotEquals(dsRef,  dsRef2);
    }

    @Test
    public void create_delete_create_dLink_2() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("create_delete_create_2", "http://example/cdc");

        // Add a patch
        patch_send(dsRef, "patch-empty.rdfp");

        // Delete
        dLink.removeDataSource(dsRef);
        // remake
        Id dsRef2 = dLink.newDataSource("create_delete_create_2", "http://example/cdc");
        assertNotEquals(dsRef,  dsRef2);
    }

    @Test
    public void dLink_events_1() {
        DeltaLink dLink = getLink();
        DeltaLinkCounter listener = new DeltaLinkCounter();
        dLink.addListener(listener);

        assertEquals(0, listener.countCreated());
        Id dsRef = dLink.newDataSource("dLink_events_1", "http://example/cde1");
        assertEquals(1, listener.countCreated());

        assertEquals(0, listener.countRemoved());
        dLink.removeDataSource(dsRef);
        assertEquals(1, listener.countRemoved());
    }

    @Test
    public void dLink_events_2() {
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("dLink_events_2", "http://example/cde2");

        DeltaLinkCounter listener = new DeltaLinkCounter();
        dLink.addListener(listener);

        assertEquals(0, listener.countFetch());
        RDFPatch p0 = dLink.fetch(dsRef, Version.create(1));
        assertEquals(1, listener.countFetch());

        assertEquals(0, listener.countAppend());
        patch_send(dsRef, "patch-empty.rdfp");
        assertEquals(1, listener.countAppend());

        assertEquals(1, listener.countFetch());
        RDFPatch p1 = dLink.fetch(dsRef, Version.create(1));
        assertEquals(2, listener.countFetch());

        assertEquals(2, listener.countFetch());
        RDFPatch p1a = dLink.fetch(dsRef, Id.fromNode(p1.getId()));
        assertEquals(3, listener.countFetch());
    }

    private static void setupEvents(DeltaLink dLink , Id dsRef,  RDFChanges rdfChanges) {
        DeltaLinkEvents.listenToPatches(dLink, dsRef, rdfChanges);
        // This listener should not happen.
        Id dsNoRef = Id.create();
        DeltaLinkEvents.listenToPatches(dLink, dsNoRef, new RDFChangesNotExpected());
    }

    @Test
    public void dLink_patch_events_1() {
        // Test routing to RDFChanges.
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("dLink_patch_events_1", "http://example/patchEvent1");

        Node gn = NodeFactory.createURI("http://example/g");
        // Test listener.
        RDFChangesCounter c = new RDFChangesCounter() {
            @Override
            public void add(Node g, Node s, Node p, Node o) {
                assertEquals(gn, g);
                super.add(g, s, p, o);
            }
        };
        setupEvents(dLink, dsRef, c);

        patch_send(dsRef, "patch1.rdfp");
        // Fetch patch, trigger listener.
        dLink.fetch(dsRef, Version.FIRST);

        // Assess what happened.
        PatchSummary summary = c.summary();
        assertEquals(1, summary.countAddData);
    }

    @Test
    public void dLink_patch_events_graph_1() {
        // Test routing to RDFChanges that routes to a graph.
        DeltaLink dLink = getLink();
        Id dsRef = dLink.newDataSource("dLink_patch_events_2", "http://example/patchEvent2");

        // Agrees with "patch1"
        Graph g = GraphFactory.createGraphMem();
        AtomicInteger counter = new AtomicInteger();
        GraphListener gl = new GraphListenerBase() {
            @Override protected void deleteEvent(Triple t) {}
            @Override protected void addEvent(Triple t) { counter.incrementAndGet(); }
        };
        g.getEventManager().register(gl);

        // Route to this graph.
        DeltaLinkEvents.enableGraphEvents(dLink, dsRef, (n)->{
            assertNull(n);
            return g;
        });

        patch_send(dsRef, "patch-graph.rdfp");

        // Fetch patch, trigger listener.
        dLink.fetch(dsRef, Version.FIRST);
        assertEquals(1, counter.get());
        dLink.fetch(dsRef, Version.FIRST);
        assertEquals(2, counter.get());
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
