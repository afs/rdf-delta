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
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.ext.com.google.common.base.Objects;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;
import org.seaborne.patch.system.DatasetGraphChanges;

/** Tests for the link (multiplex connection to the server or local engine) */
public abstract class AbstractTestDeltaLink {
    
    public abstract DeltaLink getLink();

    public abstract void reset();
    public abstract Id getDataSourceId();
    
    protected static String DIR = "testing/Delta";
    protected static String TMP = "target/Delta";
    static {
        FileOps.ensureDir(TMP);
    }
    
    protected static UUID uuid1 = UUID.randomUUID();
    protected static Id id1 = Id.fromUUID(uuid1);
    protected static UUID uuid2 = UUID.randomUUID();
    protected static Id id2 = Id.fromUUID(uuid2);
    
    @Before public void beforeTest() { reset(); }
    @After  public void afterTest() {}
    
    @Test
    public void register_01() {
        DeltaLink dLink = getLink();
        RegToken regToken = dLink.register(id2);
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

    @Test
    public void patch_01() {
        DeltaLink dLink = getLink();
        Id id1 = getDataSourceId();
        
        InputStream in = IO.openFile(DIR+"/patch1.rdfp");
        RDFPatch patch = RDFPatchOps.read(in);

        Id dsRef = getDataSourceId();
        int version = dLink.getCurrentVersion(dsRef);

        int version1 = dLink.sendPatch(dsRef, patch);
        assertNotEquals(version, version1);

        int version2 = dLink.getCurrentVersion(dsRef);
        assertEquals(version1, version2);
        
        RDFPatch patch1 = dLink.fetch(dsRef, version1) ;
        RDFPatch patch2 = dLink.fetch(dsRef, Id.fromNode(patch.getId())) ;

        assertTrue(equals(patch1, patch2));

        //check patches.
    }

    private static boolean equals(RDFPatch patch1, RDFPatch patch2) {
        RDFChangesCollector c1 = new RDFChangesCollector();
        // The getRDFPatch is a RDFPatchStored which supports hashCode and equals.
        RDFChangesCollector.RDFPatchStored p1 = (RDFChangesCollector.RDFPatchStored)c1.getRDFPatch();
        
        RDFChangesCollector c2 = new RDFChangesCollector();
        RDFChangesCollector.RDFPatchStored p2 = (RDFChangesCollector.RDFPatchStored)c2.getRDFPatch();
        
        return Objects.equal(p1, p2);
    }
    
    @Test
    public void client_01() {
        DeltaLink dLink = getLink();
        Id clientId = Id.create();
        
        // OPENS "--mem--"
        
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        DeltaConnection dConn = DeltaConnection.create("label",
                                                       clientId, getDataSourceId(),
                                                       dsg,
                                                       dLink);
        int v = dConn.getLocalVersionNumber();
        assertNotNull(dConn.getName());
        assertEquals(0, dConn.getLocalVersionNumber());
        assertEquals(0, dConn.getRemoteVersionNumber());
        assertEquals(0, dConn.getRemoteVersionLatest());
        assertNotNull(dConn.getStorage());

        // Check the DSG  
        assertEquals(dsg, dConn.getStorage());
        DatasetGraph dsg1 = dConn.getDatasetGraph();
        assertNotEquals(dsg, dsg1);
        assertTrue(dsg1 instanceof DatasetGraphChanges);
    }
}
