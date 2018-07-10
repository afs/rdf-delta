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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.server.local.PatchLog;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

/**
 * Some basic test to make sure a patch log works.
 * Most testing comes from using in the integration tests.
 */

public abstract class AbstractTestPatchLog {
    
    protected abstract PatchLog patchLog();
    
    @Test public void patchLog_1_empty() {
        PatchLog patchLog = patchLog();
        
        boolean b = patchLog.isEmpty();
        assertTrue(patchLog.isEmpty());
        
        PatchLogInfo x = patchLog.getInfo();
        assertEquals(null, x.getLatestPatch());
        assertEquals(DeltaConst.VERSION_INIT, x.getMaxVersion());
        assertEquals(DeltaConst.VERSION_INIT, x.getMinVersion());
    }
    
    @Test public void patchLog_2_singlePatch() {
        PatchLog patchLog = patchLog();
        assertTrue(patchLog.isEmpty());
        
        RDFPatch patch = RDFPatchOps.emptyPatch();
        long v = patchLog.append(patch);
        assertEquals(1, v);
        RDFPatch patch1 = patchLog.fetch(1);
        assertNotNull(patch1);
        
        PatchLogInfo x = patchLog.getInfo();
        assertEquals(patch.getId(), x.getLatestPatch().asNode());
        assertEquals(1, x.getMaxVersion());
        assertEquals(DeltaConst.VERSION_FIRST, x.getMinVersion());

        RDFPatch patch2 = patchLog.fetch(2);
        assertNull(patch2);
}

    @Test public void patchLog_3_two_patches() {
        PatchLog patchLog = patchLog();
        RDFPatch patchAdd1 = RDFPatchOps.emptyPatch();
        long v1 = patchLog.append(patchAdd1);
        assertEquals(1, v1);
        
        // Does not exist
        RDFPatch patch_missing = patchLog.fetch(2);
        assertNull(patch_missing);
        
        RDFPatch patchFetch1 = patchLog.fetch(1);
        assertNotNull(patchFetch1);
        
        RDFPatch patchAdd2 = RDFPatchOps.withHeader(patchAdd1, Id.create().asNode(), patchAdd1.getId());
        
        long v2 = patchLog.append(patchAdd2);
        assertEquals(2, v2);

        RDFPatch patch3 = patchLog.fetch(3);
        assertNull(patch3);

        RDFPatch patchFetch2 = patchLog.fetch(2);
        assertNotNull(patchFetch2);
        
        PatchLogInfo x = patchLog.getInfo();
        assertEquals(patchFetch2.getId(), x.getLatestPatch().asNode());
        assertEquals(2, x.getMaxVersion());
        assertEquals(DeltaConst.VERSION_FIRST, x.getMinVersion());
    }
}
