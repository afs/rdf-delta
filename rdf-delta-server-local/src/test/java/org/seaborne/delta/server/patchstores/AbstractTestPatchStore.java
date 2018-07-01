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

package org.seaborne.delta.server.patchstores;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import org.apache.jena.atlas.lib.FileOps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.system.DeltaSystem;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

public abstract class AbstractTestPatchStore {

    private static String DIR = "target/PatchStore"; 
    private static Path patchesArea;
    private static int counter = 0;
    private static PatchStore patchStore = null;
        
    @BeforeClass public static void setup() {
        FileOps.clearAll(DIR);
        FileOps.ensureDir(DIR);
        DeltaSystem.init();
        PatchStore.clearLogIdCache();
    }
    
    @Before public void setupTest() {
//        FileOps.clearAll(DIR);
//        FileOps.ensureDir(DIR);
    }
    
    /**
     * Return the PatchStore implementation under test. Return the same object each time.
     */
    private PatchStore provider() {
        if ( patchStore == null )
            patchStore = patchStore();
        return patchStore;
    }
    
    protected abstract PatchStore patchStore();

//    ps.release(patchLog);
//    ps.exists("ABC");

    
    @Test public void patchStore_1() {
//        PatchStore ps = provider();
//        DataSourceDescription dsdSetup = new DataSourceDescription(Id.create(), "ABC", "http://example/ABC");
//        
//        Path sourcePath = null;
//        if ( ps.hasFileArea() )
//            sourcePath = Cfg.setupDataSourceByFile(Location.create(DIR), patchStore, dsdSetup);
//        PatchLog patchLog = ps.createLog(dsdSetup, sourcePath);
    }
    
    // Recovery (does not apply to PatchStoreMem)
    //@Test 
    public void recovery1() {
        PatchStore ps = provider();

        // Match dsd2 below
        DataSourceDescription dsdSetup = new DataSourceDescription(Id.create(), "ABC", "http://example/ABC");
        PatchLog patchLog = ps.createLog(dsdSetup);
        
        RDFPatch patch = RDFPatchOps.emptyPatch();
        patchLog.append(patch);
        
        PatchLogInfo info = patchLog.getInfo();
        Id id = patchLog.getLogId();
        DataSourceDescription dsd = patchLog.getDescription();
        
        // Reset internal.
        PatchStore.clearLogIdCache();
        
        String name = dsd.getName();
        PatchStore provider = provider();

        // Same FileStore, different PatchLog?
        DataSourceDescription dsd2 = new DataSourceDescription(id, name, "http://example/ABC");
        PatchLog patchLog1 = provider.connectLog(dsd2);
        PatchLogInfo info1 = patchLog1.getInfo();
        assertEquals(info, info1);
    }
    
    // MORE TESTS
    // Get non-existent.
}
