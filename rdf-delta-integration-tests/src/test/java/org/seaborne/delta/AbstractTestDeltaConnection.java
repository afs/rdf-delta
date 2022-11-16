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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger ;
import java.util.stream.LongStream;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.FileOps;
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
import org.seaborne.delta.lib.LogX;
import org.seaborne.delta.link.DeltaLink;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.rdfpatch.changes.RDFChangesCollector;

/** Test a client connection over a link */
public abstract class AbstractTestDeltaConnection {
    // See also AbstractTestDeltaClient

    private static String DIR = "target/Zone";

    @BeforeClass public static void setupZone() {
        LogX.setJavaLogging("src/test/resources/logging.properties");
        FileOps.ensureDir(DIR);
        FileOps.clearAll(DIR);
        Zone.connect(DIR);
    }

    @AfterClass public static void cleanOutZone() {
        Zone.get(DIR).shutdown();
    }

    protected abstract Setup.LinkSetup getSetup();

    protected DeltaLink getLink() {
        return getSetup().getLink();
    }

    protected Zone getZone() { return Zone.get(DIR); }

    protected DeltaClient createDeltaClient() {
        return DeltaClient.create(getZone(), getLink());
    }

    protected DeltaClient createRegister(String name) {
        DeltaClient dClient = DeltaClient.create(getZone(), getLink());
        Id dsRef = dClient.newDataSource(name, "http://example/"+name);
        dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);
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
        dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);
        DeltaConnection dConn = dClient.get(dsRef);
        assertNotNull(dConn.getDatasetGraph());
        assertEquals(Version.INIT, dConn.getLocalVersion());
        assertEquals(Version.INIT, dConn.getRemoteVersionLatest());

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
            Version verLocal0 = dConn.getLocalVersion();
            Version verRemotel0 = dConn.getRemoteVersionLatest();

            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                dsg.add(SSE.parseQuad("(:gx :sx :px :ox)"));
            });

            Version verLocal1 = dConn.getLocalVersion();
            Version verRemotel1 = dConn.getRemoteVersionLatest();
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
            Version version = dConn.getRemoteVersionLatest();

            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s1 :p1 :o1)");
                dsg.add(q);
            });
            // Rebuild directly.
            DatasetGraph dsg2 = DatasetGraphFactory.createTxnMem();
            Version ver = dConn.getRemoteVersionLatest();
            RDFPatch patch1 = dConn.getLink().fetch(dsRef, ver) ;
            RDFPatchOps.applyChange(dsg2, patch1);

            Set<Quad> set1 = Txn.calculateRead(dsg, ()->Iter.toSet(dsg.find()));
            Set<Quad> set2 = Txn.calculateRead(dsg2, ()->Iter.toSet(dsg2.find()));

            assertEquals(set1, set2);
        }
    }

    // Make a change, make another change
    @Test
    public void change_3() {
        String NAME = "change_3";
        DeltaClient dClient = createRegister(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            Id dsRef = dConn.getDataSourceId();
            Version version = dConn.getRemoteVersionLatest();

            DatasetGraph dsg = dConn.getDatasetGraph();
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s1 :p1 :o1)");
                dsg.add(q);
            });
            Txn.executeWrite(dsg, ()->{
                Quad q = SSE.parseQuad("(_ :s2 :p2 :o2)");
                dsg.add(q);
            });

            // Rebuild directly.
            DatasetGraph dsg2 = DatasetGraphFactory.createTxnMem();
            Version ver = dConn.getRemoteVersionLatest();
            PatchLogInfo info = dConn.getLink().getPatchLogInfo(dsRef);
            LongStream.rangeClosed(info.getMinVersion().value(), info.getMaxVersion().value()).forEach(v->{
                RDFPatch patch = dConn.getLink().fetch(dsRef, Version.create(v)) ;
                RDFPatchOps.applyChange(dsg2, patch);
            });

            Set<Quad> set1 = Txn.calculateRead(dsg, ()->Iter.toSet(dsg.find()));
            Set<Quad> set2 = Txn.calculateRead(dsg2, ()->Iter.toSet(dsg2.find()));
            assertEquals(set1, set2);
        }
    }

    @Test
    public void change_empty_commit_1() {
        Quad q = SSE.parseQuad("(:g :s :p :o)") ;
        String NAME = "change_empty_commit_1";
        DeltaClient dClient = createRegister(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {

            Id patchId0 = dConn.getRemoteIdLatest();
            assertNull(patchId0);
            Version ver0 = dConn.getRemoteVersionLatest();

            // The "no empty commits" dsg
            DatasetGraph dsg = dConn.getDatasetGraphNoEmpty();

            Txn.executeWrite(dsg, ()->{});
            Id patchId1 = dConn.getLatestPatchId();
            Version ver1 = dConn.getRemoteVersionLatest();
            // No change at start of log.
            assertEquals(patchId0, patchId1);
            assertEquals(ver0, ver1);

            Txn.executeWrite(dsg, ()->dsg.add(q));
            Id patchId2 = dConn.getLatestPatchId();
            Version ver2 = dConn.getRemoteVersionLatest();
            assertNotEquals(patchId0, patchId2);
            assertNotEquals(ver0, ver2);

            DatasetGraph dsgx = dConn.getDatasetGraph();
            Txn.executeWrite(dsgx, ()->{});
            Id patchId3 = dConn.getLatestPatchId();
            Version ver3 = dConn.getRemoteVersionLatest();
            assertNotEquals(patchId2, patchId3);
            assertNotEquals(ver2, ver3);

            // No change mid log.
            Txn.executeWrite(dsg, ()->Iter.count(dsg.find()));
            Id patchId4 = dConn.getLatestPatchId();
            Version ver4 = dConn.getRemoteVersionLatest();
            assertEquals(patchId3, patchId4);
            assertEquals(ver3, ver4);
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

    // ---- Same dataset carried across connections

    @Test public void change_read_same_1() {
        change_read_same(()->{});
    }

    @Test public void change_read_same_2() {
        change_read_same(()->getSetup().relink());
    }

    @Test public void change_read_same_3() {
        if ( getSetup().restartable() )
            change_read_same(()->getSetup().restart());
    }

    private static AtomicInteger counter = new AtomicInteger(0);
    private DeltaClient resetDeltaClient(String name) {
        DeltaClient dClient = createDeltaClient();
        Id dsRef = dClient.nameToId(name);
        if ( ! dClient.getZone().exists(dsRef) )
            dClient.register(dsRef, LocalStorageType.MEM, SyncPolicy.NONE);
        else
            dClient.connect(dsRef, SyncPolicy.NONE);
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

        Version verLocal = Version.UNSET;
        Version verRemote = Version.UNSET;
        Id dsRef;

        String NAME = "DS-"+counter.incrementAndGet();

        DeltaClient dClient = createRegister(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            dsRef = dConn.getDataSourceId();
            dsgBase = dConn.getStorage();
            DatasetGraph dsg = dConn.getDatasetGraph();
            Version ver = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(Version.INIT, ver);
            // Make change.
            Txn.executeWrite(dsg, ()->dsg.add(quad));
            verLocal = dConn.getLocalVersion();
            assertEquals(ver.value()+1, verLocal.value());
        }

        betweenSections.run();

        // New client.
        dClient = resetDeltaClient(NAME);
        // Same name.
        // Zone should have found (or not lost) the existing setup.
        // dClient.connect(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            Version ver = dConn.getLocalVersion();
            Version ver2 = dConn.getRemoteVersionLatest();

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
            Version version = dConn.getRemoteVersionLatest();
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
        if ( getSetup().restartable() )
            change_change_read_Same(()->{});
    }

    @Test public void change_change_read_same_2() {
        if ( getSetup().restartable() )
            change_change_read_Same(()->getSetup().relink());
    }

    @Test public void change_change_read_same_3() {
        if ( getSetup().restartable() )
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
        if ( getSetup().restartable() )
            change_change_read_Same(betweenSections, betweenSections);
    }

    private void change_change_read_Same(Runnable betweenSections1, Runnable betweenSections2) {
        Quad quad1 = DeltaTestLib.freshQuad();
        Quad quad2 = DeltaTestLib.freshQuad();
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();

        Version verLocal = Version.UNSET;
        Version verRemote = Version.UNSET;
        Id dsRef;

        String NAME = "DS-"+counter.incrementAndGet();
        DeltaClient dClient = createRegister(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            dsRef = dConn.getDataSourceId();
            dsgBase = dConn.getStorage();
            DatasetGraph dsg = dConn.getDatasetGraph();
            Version ver = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(Version.INIT, ver);
            // Make change.
            Txn.executeWrite(dsg, ()->dsg.add(quad1));
            verLocal = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
            assertEquals(ver.value()+1, verLocal.value());
        }

        betweenSections1.run();

        // Reconnect, make second change.
        dClient = resetDeltaClient(NAME);

        try(DeltaConnection dConn = dClient.get(NAME)) {
            DatasetGraph dsg = dConn.getDatasetGraph();

            Txn.executeWrite(dsg, ()->dsg.add(quad2));

            Version ver = dConn.getLocalVersion();
            Version ver2 = dConn.getRemoteVersionLatest();

            assertEquals(verLocal.value()+1, ver.value());
            assertEquals(verRemote.value()+1, ver2.value());

            verLocal = dConn.getLocalVersion();
            verRemote = dConn.getRemoteVersionLatest();
        }

        betweenSections2.run();

        // Reconnect and read
        dClient = resetDeltaClient(NAME);
        try(DeltaConnection dConn = dClient.get(NAME)) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            Version ver = dConn.getLocalVersion();
            Version ver2 = dConn.getRemoteVersionLatest();

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
//        Version verLocal = -999;
//        Version verRemote = -999;
//        Id dsRef;
//
//        try(DeltaConnection dConn = create(dsgBase)) {
//            dsRef = dConn.getDatasourceId();
//            dsgBase = dConn.getStorage();
//            DatasetGraph dsg = dConn.getDatasetGraph();
//            Version ver = dConn.getLocalVersionNumber();
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
//            Version ver = dConn.getLocalVersionNumber();
//            Version ver2 = dConn.getRemoteVersionLatest();
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
//            Version ver = dConn.getLocalVersionNumber();
//            Version ver2 = dConn.getRemoteVersionLatest();
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
