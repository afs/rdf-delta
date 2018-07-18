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

package org.seaborne.delta.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.seaborne.delta.systemtest.Matrix.deltaServerURL1;
import static org.seaborne.delta.systemtest.Matrix.deltaServerURL2;

import java.util.Arrays;

import org.apache.jena.atlas.logging.LogCtl;
import org.junit.*;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.Version;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.DeltaLinkSwitchable;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.systemtest.Matrix;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;


// These tests build from simple to complex.
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TestDeltaZk {
    
    // remove when suite ready
    static { 
        LogCtl.setJavaLogging("logging-test.properties");
    }
    
    @BeforeClass public static void beforeClass() { DeltaLinkSwitchable.silentSwitchOver = true; }
    @AfterClass public static void afterClass()   { DeltaLinkSwitchable.silentSwitchOver = false; } 
    
    @Before public void before() { Matrix.setup(); }
    @After  public void after()  { Matrix.teardown(); }

    // 2 delta/zk servers, create a log on one link, check it exists via the other. 
    @Test
    public void t1_twoLinks_create() {
        DeltaLink dLink1 = DeltaLinkHTTP.connect(deltaServerURL1);
        DeltaLink dLink2 = DeltaLinkHTTP.connect(deltaServerURL2);
        
        String NAME = "ABC1";
        
        assertFalse("Setup not clean (1)", dLink1.existsByName(NAME));
        assertFalse("Setup not clean (2)", dLink2.existsByName(NAME));
        
        // Create on 1.
        Id logId = dLink1.newDataSource(NAME, "http://example/abc");
        Matrix.await(()->dLink1.existsByName(NAME));

        assertTrue("Setup - didn't create patch log (1)", dLink1.existsByName(NAME));
        // XXX Retry?
        assertTrue("Setup - didn't create patch log (2)", dLink2.existsByName(NAME));
    }
    
    // Create and delete a patch log on one of the links.
    @Test
    public void t2_oneLink_create_delete() {
        String NAME = "ABC2";
        // One zk
        DeltaLink dLink = DeltaLinkHTTP.connect(deltaServerURL1);
        assertFalse("Setup not clean (1)", dLink.existsByName(NAME));
        
        Id logId = dLink.newDataSource(NAME, "http://example/abc");

        assertTrue("Setup - didn't create patch log (1)", dLink.existsByName(NAME));
        
        dLink.removeDataSource(logId);
        assertFalse("Remove - patch log still exists (1)", dLink.existsByName(NAME));
    }
    
    // 2 delta/zk, create a log on one link, check it exists via the other; delete on original link, test.
    @Test
    public void t3_twoLinks_create_delete() {
        String NAME = "ABC3";
        DeltaLink dLink1 = DeltaLinkHTTP.connect(deltaServerURL1);
        DeltaLink dLink2 = DeltaLinkHTTP.connect(deltaServerURL2);
        
        assertFalse("Setup not clean (1)", dLink1.existsByName(NAME));
        assertFalse("Setup not clean (2)", dLink2.existsByName(NAME));
        
        // Create on 1.
        Id logId = dLink1.newDataSource(NAME, "http://example/abc");
        Matrix.await(()->dLink2.existsByName(NAME));
        
        assertTrue("Setup - didn't create patch log (1)", dLink1.existsByName(NAME));
        // Need to retry this?
        assertTrue("Setup - didn't create patch log (2)", dLink2.existsByName(NAME));
        
        // Delete on 1.
        dLink1.removeDataSource(logId);
        assertFalse("Remove - patch log still exists (1)", dLink1.existsByName(NAME));
        assertFalse("Remove - patch log still exists (2)", dLink2.existsByName(NAME));
    }

    // 2 servers, DeltaLinkSwitchable over two links. Stop one delta server.
    @Test
    public void t4_switchable_append() {
        String NAME = "ABC4";

        DeltaLink dLink1 = DeltaLinkHTTP.connect(deltaServerURL1);
        DeltaLink dLink2 = DeltaLinkHTTP.connect(deltaServerURL2);

        assertFalse("Setup not clean", dLink1.existsByName(NAME));
        Id logId = dLink2.newDataSource(NAME, "http://example/abc");
        Matrix.await(()->dLink1.existsByName(NAME));
        
        assertTrue("Setup - didn't create patch log", dLink1.existsByName(NAME));

        RDFPatch patch_i1 = RDFPatchOps.read("data.rdfp");
        // XXX Add a RDFPatchOps.patchAfter(RDFPatch patch);
        RDFPatch patch_i2 = RDFPatchOps.withHeader(patch_i1, Id.create().asNode(), patch_i1.getId());
        
        DeltaLinkSwitchable dLink = new DeltaLinkSwitchable(Arrays.asList(dLink1, dLink2));
        
        Version ver1 = dLink.append(logId, patch_i1);
        RDFPatch patch1 = dLink1.fetch(logId, ver1);
        assertNotNull(patch1);

        Matrix.deltaServer1.stop();
        // Manual force.
        //dLink.switchover();

        RDFPatch patch2 = dLink.fetch(logId, ver1);
        assertNotNull(patch2);

        Version ver2 = dLink.append(logId, patch_i2);
        
        
        assertEquals(Version.create(2), ver2);
        
        PatchLogInfo info2 = dLink.getPatchLogInfo(logId);
        assertEquals(Version.create(2), info2.getMaxVersion());
        assertEquals(patch_i2.getId(), info2.getLatestPatch().asNode());
    }
    
        
}
