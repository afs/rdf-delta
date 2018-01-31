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

package org.seaborne.delta.server;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.atlas.lib.FileOps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.server.local.patchlog.PatchLog;
import org.seaborne.delta.server.local.patchlog.PatchStore;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

public abstract class AbstractTestPatchStore {
    //new PatchStoreFile
    private static String DIR = "target/PatchStore"; 
    private static Path patchesArea;
    private static int counter = 0;
    private static PatchStore provider = null;
        
    @BeforeClass public static void setup() {
        FileOps.clearAll(DIR);
        FileOps.ensureDir(DIR);
        PatchStore.clearLogIdCache();
    }
    
    @Before public void setupTest() {
//        FileOps.clearAll(DIR);
//        FileOps.ensureDir(DIR);
    }
    
    /** Return the PatchStore implementation under test.
     *  Return the same object each time. */
    private PatchStore provider() {
        if ( provider == null )
            provider = createProvider();
        return provider;
    }
    
    protected abstract PatchStore createProvider();
    
    private PatchLog patchLog() {
        PatchStore patchStore = provider();
        Id dsRef = Id.create();
        String name = "patch-store-"+(++counter);
        Path patchesArea = Paths.get(DIR, name);
        DataSourceDescription dsd = new DataSourceDescription(dsRef, name, null);
        PatchLog patchLog = patchStore.createLog(dsd, patchesArea);
        return patchLog;
    }
    
    @Test public void ps1() {
        PatchLog patchLog = patchLog();
        
        boolean b = patchLog.isEmpty();
        long x = patchLog.getEarliestVersion();
        
        assertTrue(patchLog.isEmpty());
    }
    
    @Test public void ps2() {
        PatchLog patchLog = patchLog();
        assertTrue(patchLog.isEmpty());
        RDFPatch patch = RDFPatchOps.emptyPatch();
        patchLog.append(patch);
        assertFalse(patchLog.isEmpty());
    }
    
    @Test public void ps3() {
        PatchLog patchLog = patchLog();
        RDFPatch patch = RDFPatchOps.emptyPatch();
        long v = patchLog.append(patch);
        assertEquals(1, v);
        
        RDFPatch patch1 = patchLog.fetch(1);
        assertNotNull(patch1);
        RDFPatch patch2 = patchLog.fetch(2);
        assertNull(patch2);
    }

    @Test public void ps4() {
        PatchLog patchLog = patchLog();
        RDFPatch patch = RDFPatchOps.emptyPatch();
        patchLog.append(patch);
        // Does not exist
        RDFPatch patch2 = patchLog.fetch(2);
        assertNull(patch2);
    }
    
    // Recovery (does not apply to PatchStoreMem)
    @Test public void recovery1() {
        PatchLog patchLog = patchLog();
        RDFPatch patch = RDFPatchOps.emptyPatch();
        
       // header not written.
        
        patchLog.append(patch);
        PatchLogInfo info = patchLog.getDescription();
        Id id = info.getDataSourceId();
        
        // Reset internal.
        PatchStore.clearLogIdCache();
        
        String name = info.getDataSourceName();
        PatchStore provider = provider();
        Path patchesArea = Paths.get(DIR, info.getDataSourceName()); 

        // Same FileStore, different PatchLog?
        DataSourceDescription dsd = new DataSourceDescription(id, name, null);
        PatchLog patchLog1 = provider.connectLog(dsd, patchesArea);
        
        PatchLogInfo info1 = patchLog1.getDescription();
        assertEquals(info, info1);
    }
    // Get non-existent.
}
